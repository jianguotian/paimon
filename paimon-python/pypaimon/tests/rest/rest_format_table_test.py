# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pytest
from parameterized import parameterized

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import TableNotExistException
from pypaimon.schema.data_types import AtomicType, DataField, MultisetType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.format import FormatTable
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


def _format_table_read_write_formats():
    formats = [("parquet",), ("csv",), ("json",)]
    if hasattr(pa, "orc"):
        formats.append(("orc",))
    return formats


class RESTFormatTableTest(RESTBaseTest):

    @staticmethod
    def _write_arrow(table, data):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def _assert_format_read(self, table, expected, projection=None):
        builder = table.new_read_builder()
        if projection is not None:
            builder.with_projection(projection)
        self.assertEqual([field.name for field in builder.read_type()], expected.column_names)
        splits = builder.new_scan().plan().splits()
        reader = builder.new_read()
        self.assertEqual(reader.to_arrow(splits), expected)
        self.assertEqual(pa.Table.from_batches(list(reader.to_iterator(splits))), expected)

    @parameterized.expand(_format_table_read_write_formats())
    def test_format_table_read_write(self, file_format):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
            ("c", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"type": "format-table", "file.format": file_format},
        )
        table_name = f"default.format_table_rw_{file_format}"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        self.assertIsInstance(table, FormatTable)
        self.assertEqual(table.format().value, file_format)
        opts = table.options()
        self.assertIsInstance(opts, dict)
        self.assertEqual(opts.get("file.format"), file_format)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        df = pd.DataFrame({
            "a": [10, 10],
            "b": [1, 2],
            "c": [1, 2],
        })
        table_write.write_pandas(df)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        table_read = read_builder.new_read()
        actual = table_read.to_pandas(splits).sort_values(by="b").reset_index(drop=True)
        expected = pa.Table.from_pydict(
            {"a": [10, 10], "b": [1, 2], "c": [1, 2]},
            schema=pa_schema,
        ).to_pandas()
        for col in expected.columns:
            if col in actual.columns and actual[col].dtype != expected[col].dtype:
                actual[col] = actual[col].astype(expected[col].dtype)
        pd.testing.assert_frame_equal(actual, expected)

    @parameterized.expand([("parquet",), ("orc",)])
    def test_format_table_read_added_column(self, file_format):
        if file_format == "orc":
            pytest.importorskip("pyarrow.orc")

        name = "default.evolving_format_table"
        schema = Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int64())]),
            options={"type": "format-table", "file.format": file_format})
        self.rest_catalog.create_table(name, schema, False)

        # External files can contain columns that were never part of the catalog schema.
        self._write_arrow(self.rest_catalog.get_table(name), pa.table({"id": [1, 2], "_source_row": [8, 9]}))
        old_split, = self.rest_catalog.get_table(name).new_read_builder().new_scan().plan().splits()
        self.rest_catalog.alter_table(name, [
            SchemaChange.add_column("campaign", AtomicType("STRING")),
            SchemaChange.add_column("created", AtomicType("TIMESTAMP(3)"))])
        physical_schema = pa.schema([
            ("id", pa.int64()), pa.field("campaign", pa.string(), nullable=False), ("created", pa.timestamp("ms"))])
        self._write_arrow(self.rest_catalog.get_table(name), pa.table(
            {"id": [3], "campaign": ["spring"], "created": [123]}, schema=physical_schema))

        builder = self.rest_catalog.get_table(name).new_read_builder()
        splits = builder.new_scan().plan().splits()
        new_split, = [split for split in splits if split != old_split]
        ordered = [old_split, new_split]
        created = pa.array([None, None, 123], pa.timestamp("ms"))
        if file_format == "orc":
            created = created.cast(pa.timestamp("ns"))
        expected = pa.table({"id": [1, 2, 3], "campaign": [None, None, "spring"], "created": created})
        self.assertEqual(builder.new_read().to_arrow(ordered), expected)
        self.assertEqual(builder.new_read().to_arrow(ordered[::-1]), expected.take([2, 0, 1]))
        reader = builder.with_projection(["created", "campaign"]).new_read()
        projected = expected.select(["created", "campaign"])
        self.assertEqual(pa.concat_tables([reader.to_arrow([s]) for s in ordered]), projected)
        batches = [batch for s in ordered for batch in reader.to_iterator([s])]
        self.assertEqual(pa.Table.from_batches(batches), projected)
        self.assertEqual(reader.to_arrow([]).schema, projected.schema)

        limited = builder.with_limit(1).new_read()
        file_io = limited.table.file_io
        for read in (limited.to_arrow, lambda s: next(limited.to_iterator(s))):
            with patch.object(file_io, "new_input_stream", wraps=file_io.new_input_stream) as opened:
                self.assertEqual(read(ordered).to_pydict(), {"created": [None], "campaign": [None]})
                opened.assert_called_once_with(old_split.data_path())

    def test_parquet_evolved_nullable_integer_pandas_dtype(self):
        name = "default.pandas_integer"
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int64())]), options={"type": "format-table", "file.format": "parquet"}), False)
        self._write_arrow(self.rest_catalog.get_table(name), pa.table({"id": [0]}))
        old, = self.rest_catalog.get_table(name).new_read_builder().new_scan().plan().splits()
        self.rest_catalog.alter_table(name, [SchemaChange.add_column("amount", AtomicType("BIGINT"))])
        table = self.rest_catalog.get_table(name)
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        large = 2 ** 53 + 1
        try:
            writer.write_pandas(pd.DataFrame({"id": [1, 2], "amount": pd.array([large, None], dtype="Int64")}))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
        read = table.new_read_builder().with_projection(["amount"])
        new, = [s for s in read.new_scan().plan().splits() if s != old]
        for splits, values in (([new], [large, None]), ([old, new], [None, large, None]),
                               ([new, old], [large, None, None])):
            expected = pd.DataFrame({"amount": pd.array(values, dtype="Int64")})
            pd.testing.assert_frame_equal(read.new_read().to_pandas(splits), expected)

    @parameterized.expand([("parquet",), ("orc",)])
    def test_directory_partition_overrides_physical_column(self, file_format):
        if file_format == "orc":
            codec = pytest.importorskip("pyarrow.orc")
        else:
            import pyarrow.parquet as codec
        name = "default.partition_source"
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int64()), ("pt", pa.string())]), partition_keys=["pt"],
            options={"type": "format-table", "file.format": file_format}), False)
        table = CatalogFactory.create({**self.options, "case-sensitive": "false"}).get_table(name)
        directory = table.location() + "/pt=west"
        table.file_io.check_or_mkdirs(directory)
        with table.file_io.new_output_stream(directory + "/part." + file_format) as stream:
            codec.write_table(pa.table({"id": [1], "PT": ["east"]}), stream)
        builder = table.new_read_builder().with_partition_filter({"pt": "west"}).with_projection(["pt"])
        splits = builder.new_scan().plan().splits()
        reader = builder.new_read()
        self.assertEqual(reader.to_arrow(splits).to_pydict(), {"pt": ["west"]})
        self.assertEqual(next(reader.to_iterator(splits)).to_pydict(), {"pt": ["west"]})

    @parameterized.expand([("parquet", "NUMERIC"), ("orc", "DEC")])
    def test_columnar_decimal_alias(self, file_format, alias):
        if file_format == "orc":
            pytest.importorskip("pyarrow.orc")
        name = "default.decimal_alias"
        self.rest_catalog.create_table(name, Schema(
            fields=[DataField(0, "amount", AtomicType(alias + "(10, 2)"))],
            options={"type": "format-table", "file.format": file_format}), False)
        table = self.rest_catalog.get_table(name)
        expected = pa.table({"amount": pa.array([Decimal("1.23")], pa.decimal128(10, 2))})
        self._write_arrow(table, expected)
        self._assert_format_read(table, expected)

    def test_parquet_shredded_variant(self):
        from pypaimon.data.generic_variant import GenericVariant
        name = "default.variant_table"
        self.rest_catalog.create_table(name, Schema(
            fields=[DataField(0, "v", AtomicType("VARIANT"))],
            options={"type": "format-table", "file.format": "parquet"}), False)
        table = self.rest_catalog.get_table(name)
        physical = pa.struct([("metadata", pa.binary()), ("value", pa.binary()), ("typed_value", pa.bool_())])
        value = GenericVariant.from_python(True)
        rows = [{"metadata": value.metadata(), "typed_value": True}, None]
        self._write_arrow(table, pa.table({"v": pa.array(rows, type=physical)}))
        self._assert_format_read(table, pa.table({"v": GenericVariant.to_arrow_array([value, None])}))

    def test_parquet_binary_and_time_partition_round_trip(self):
        name = "default.typed_partitions"
        schema = pa.schema([("id", pa.int64()), ("key", pa.binary()), ("clock", pa.time64("us"))])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            schema, partition_keys=["key", "clock"], options={"type": "format-table", "file.format": "parquet"}), False)
        table = self.rest_catalog.get_table(name)
        data = pa.table({"id": [1], "key": [b"\xff'"], "clock": [1234567]}, schema=schema)
        self._write_arrow(table, data)
        expected = data.set_column(2, "clock", data.column("clock").cast(pa.time64("ns")))
        self._assert_format_read(table, expected)

    @parameterized.expand([("parquet", "false"), ("orc", "true")])
    def test_escaped_directory_partition_filter(self, file_format, only_value):
        from pypaimon.filesystem.local_file_io import LocalFileIO
        if file_format == "orc":
            codec = pytest.importorskip("pyarrow.orc")
        else:
            import pyarrow.parquet as codec
        name = "default.escaped_partitions"
        schema = pa.schema([("id", pa.int64()), ("pt", pa.string())])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            schema, partition_keys=["pt"], options={"type": "format-table", "file.format": file_format,
                                                    "format-table.partition-path-only-value": only_value}), False)
        table = self.rest_catalog.get_table(name)
        root = LocalFileIO._to_file(table.location())
        for i, (component, value) in enumerate([("a%2Fb", "a/b"), ("a%252Fb", "a%2Fb")]):
            directory = root / (component if only_value == "true" else "pt=" + component)
            directory.mkdir()
            codec.write_table(pa.table({"id": [i], "pt": [value]}), str(directory / ("external." + file_format)))
        # A Python file written in the same physical directory can contain a literal percent escape.
        codec.write_table(pa.table({"id": [2], "pt": ["a%2Fb"]}),
                          str(root / ("a%2Fb" if only_value == "true" else "pt=a%2Fb") / ("literal." + file_format)))
        for value, ids in [("a/b", [0]), ("a%2Fb", [1, 2])]:
            builder = table.new_read_builder().with_partition_filter({"pt": value})
            splits = builder.new_scan().plan().splits()
            self.assertIn(value, [split.partition["pt"] for split in splits])
            reader = builder.new_read()
            self.assertEqual(sorted(reader.to_arrow(splits).column("id").to_pylist()), ids)
            self.assertEqual([row.column(1)[0].as_py() for row in reader.to_iterator(splits)], [value] * len(ids))

    @parameterized.expand([("large_string", pa.large_string(), "a%2Fb"),
                           ("fixed_binary", pa.binary(5), b"a%2Fb"),
                           ("large_binary", pa.large_binary(), b"a%2Fb"),
                           ("default_marker", pa.string(), "%5F_DEFAULT_PARTITION__")])
    def test_parquet_literal_partition_representations(self, label, physical_type, value):
        import pyarrow.parquet as pq
        from pypaimon.filesystem.local_file_io import LocalFileIO
        name = "default.literal_partition"
        schema = pa.schema([("id", pa.int64()), ("pt", physical_type)])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            schema, partition_keys=["pt"], options={"type": "format-table", "file.format": "parquet"}), False)
        table = self.rest_catalog.get_table(name)
        root = LocalFileIO._to_file(table.location())
        directory = root / ("pt=" + str(value))
        directory.mkdir()
        pq.write_table(pa.table({"id": [1], "pt": [value]}, schema=schema), str(directory / "old.parquet"))
        for partition_filter in (None, {"pt": value}):
            builder = table.new_read_builder().with_partition_filter(partition_filter)
            splits = builder.new_scan().plan().splits()
            self.assertEqual(builder.new_read().to_arrow(splits).column("pt").to_pylist(), [value])

    def test_parquet_nanosecond_partition_legacy_and_new_files(self):
        import pyarrow.parquet as pq
        name = "default.nanos_partition"
        schema = pa.schema([("id", pa.int64()), ("clock", pa.time64("ns"))])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            schema, partition_keys=["clock"], options={"type": "format-table", "file.format": "parquet"}), False)
        table = self.rest_catalog.get_table(name)
        ticks = [1001001, 1001999]
        data = pa.table({"id": [1, 2], "clock": ticks}, schema=schema)
        directory = table.location() + "/clock=00:00:00.001001"
        table.file_io.check_or_mkdirs(directory)
        with table.file_io.new_output_stream(directory + "/legacy.parquet") as stream:
            # A genuinely conflicting physical key must still resolve to the directory.
            pq.write_table(pa.table({"id": [1, 2, 4], "clock": ticks + [2001999]}, schema=schema), stream)
        builder = table.new_read_builder()
        legacy, = builder.new_scan().plan().splits()
        reader = builder.new_read()
        expected = ticks + [1001000]
        self.assertEqual(reader.to_arrow([legacy]).column("clock").cast(pa.int64()).to_pylist(), expected)
        self.assertEqual([row.column(1)[0].cast(pa.int64()).as_py() for row in reader.to_iterator([legacy])], expected)
        self._write_arrow(table, data)
        # External writers may use a full-precision directory with no physical partition field.
        exact = table.location() + "/clock=00:00:00.001001001"
        table.file_io.check_or_mkdirs(exact)
        with table.file_io.new_output_stream(exact + "/external.parquet") as stream:
            pq.write_table(pa.table({"id": [3]}), stream)
        for value, ids in [("00:00:00.001001001", [1, 1, 3]), ("00:00:00.001001999", [2, 2]),
                           ("00:00:00.001001", [4]), ("00:00:00.001002", [])]:
            scan = table.new_read_builder().with_partition_filter({"clock": value}).new_scan()
            selected = scan.plan().splits()
            projected = table.new_read_builder().with_projection(["id"]).new_read()
            self.assertEqual(sorted(projected.to_arrow(selected).column("id").to_pylist()), ids)

    def test_parquet_default_partition(self):
        import pyarrow.parquet as pq
        name = "default.null_partition"
        schema = pa.schema([("id", pa.int64()), ("pt", pa.int64())])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            schema, partition_keys=["pt"], options={"type": "format-table", "file.format": "parquet",
                                                    "partition.default-name": "__HIVE_DEFAULT_PARTITION__"}), False)
        table = self.rest_catalog.get_table(name)
        directory = table.location() + "/pt=__HIVE_DEFAULT_PARTITION__"
        table.file_io.check_or_mkdirs(directory)
        with table.file_io.new_output_stream(directory + "/part.parquet") as stream:
            pq.write_table(pa.table({"id": [1], "pt": [23]}), stream)
        self._assert_format_read(table, pa.table({"id": [1], "pt": [None]}, schema=schema))

    @parameterized.expand([("parquet",), ("orc",)])
    def test_millisecond_directory_partitions(self, file_format):
        if file_format == "orc":
            pytest.importorskip("pyarrow.orc")
        name = "default.partition_millis"
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(3)")), DataField(1, "t", AtomicType("TIME(3)"))]
        self.rest_catalog.create_table(name, Schema(
            fields=fields, partition_keys=["ts", "t"], options={"type": "format-table", "file.format": file_format}),
            False)
        table = self.rest_catalog.get_table(name)
        expected = pa.table({"ts": pa.array([1001], pa.timestamp("ms")), "t": pa.array([1001], pa.time32("ms"))})
        if file_format == "parquet":
            self._write_arrow(table, expected)
        else:
            # ORC represents TIME as an integer and external writers may omit directory keys.
            import pyarrow.orc as orc
            directory = table.location() + "/ts=1970-01-01 00:00:01.001000/t=00:00:01.001000"
            table.file_io.check_or_mkdirs(directory)
            with table.file_io.new_output_stream(directory + "/part.orc") as stream:
                orc.write_table(pa.table({"other": [1]}), stream)
        self._assert_format_read(table, expected)

    @parameterized.expand([("parquet",), ("orc",)])
    def test_columnar_case_sensitive_catalog(self, file_format):
        if file_format == "orc":
            pytest.importorskip("pyarrow.orc")
        name = "default.mixed_case"
        nested = pa.list_(pa.struct([("amount", pa.int32())]))
        logical = pa.schema([("id", pa.int64()), ("items", nested), ("campaign", pa.string())])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            logical, options={"type": "format-table", "file.format": file_format}), False)
        table = self.rest_catalog.get_table(name)
        items = pa.array([[{"Amount": 7}], None], type=pa.list_(pa.struct([("Amount", pa.int32())])))
        self._write_arrow(table, pa.table({"ID": [42, 43], "items": items}))
        self._assert_format_read(table, pa.table({"id": pa.nulls(2, pa.int64())}), ["id"])
        catalog = CatalogFactory.create({**self.options, "case-sensitive": "false"})
        expected = pa.table({"id": [42, 43], "items": [[{"amount": 7}], None],
                             "campaign": [None, None]}, schema=logical)
        table = catalog.get_table(name)
        self._assert_format_read(table, expected)
        projection = ["items", "campaign", "id"]
        self._assert_format_read(table, expected.select(projection), ["Items", "Campaign", "ID"])

    @parameterized.expand([(None, "named"), (1, "named"), (2, "positional")])
    def test_orc_positional_evolution(self, levels, nested_value):
        pytest.importorskip("pyarrow.orc")
        name = "default.positional"
        nested = pa.struct([("first", pa.string()), ("second", pa.string())])
        logical = pa.schema([("id", pa.int64()), ("pt", pa.string()),
                             ("label", nested), ("campaign", pa.string())])
        options = {"type": "format-table", "file.format": "orc"}
        if levels is not None:
            options["orc.force.positional.evolution"] = "true"
            options["orc.force.positional.evolution.level"] = str(levels)
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            logical, partition_keys=["pt"], options=options), False)
        table = self.rest_catalog.get_table(name)
        # The legacy file has no partition field. The scan supplies it from the directory.
        import pyarrow.orc as orc
        directory = table.location() + "/pt=west"
        table.file_io.check_or_mkdirs(directory)
        physical = pa.struct([("second", pa.string()), ("first", pa.string())])
        labels = pa.array([{"second": "positional", "first": "named"}], type=physical)
        data = pa.table({"_col0": [42], "_col1": labels})
        buffer = pa.BufferOutputStream()
        orc.write_table(data, buffer)
        with table.file_io.new_output_stream(directory + "/legacy.orc") as stream:
            stream.write(buffer.getvalue().to_pybytes())
        label = {"first": nested_value, "second": "named" if levels == 2 else "positional"}
        expected = pa.table({"id": [42], "pt": ["west"], "label": [label], "campaign": [None]}, schema=logical)
        self._assert_format_read(table, expected)
        projection = ["campaign", "label", "pt"]
        self._assert_format_read(table, expected.select(projection), projection)

    @parameterized.expand([("parquet",), ("orc",)])
    def test_columnar_multiset(self, file_format):
        if file_format == "orc":
            pytest.importorskip("pyarrow.orc")
        name = "default.multiset_table"
        self.rest_catalog.create_table(name, Schema(
            fields=[DataField(0, "counts", MultisetType(True, AtomicType("STRING")))],
            options={"type": "format-table", "file.format": file_format}), False)
        table = self.rest_catalog.get_table(name)
        multiset = pa.map_(pa.string(), pa.field("value", pa.int32(), nullable=False))
        expected = pa.table({"counts": pa.array([[("red", 2)], None], type=multiset)})
        self._write_arrow(table, expected)
        self._assert_format_read(table, expected)

    def test_parquet_submillisecond_time(self):
        name = "default.precise_time"
        data = pa.table({"micros": pa.array([1001], pa.time64("us")),
                         "nanos": pa.array([1001001], pa.time64("ns"))})
        schema = Schema(fields=[DataField(0, "micros", AtomicType("TIME(6)")),
                                DataField(1, "nanos", AtomicType("TIME(9)"))],
                        options={"type": "format-table", "file.format": "parquet"})
        self.rest_catalog.create_table(name, schema, False)
        table = self.rest_catalog.get_table(name)
        self._write_arrow(table, data)
        self._assert_format_read(table, data)

    def test_parquet_time_from_older_catalog_schema(self):
        name = "default.legacy_time"
        schema = Schema(fields=[DataField(0, "t", AtomicType("TIME(0)"))],
                        options={"type": "format-table", "file.format": "parquet"})
        self.rest_catalog.create_table(name, schema, False)
        table = self.rest_catalog.get_table(name)
        self._write_arrow(table, pa.table({"t": pa.array([1001], pa.time64("us"))}))
        self._write_arrow(table, pa.table({"t": pa.array([1001001], pa.time64("ns"))}))
        builder = table.new_read_builder()
        reader = builder.new_read()
        pieces = [reader.to_arrow([split]) for split in builder.new_scan().plan().splits()]
        values = pa.concat_tables(pieces).column("t")
        self.assertEqual(values.type, pa.time64("ns"))
        self.assertCountEqual(values.cast(pa.int64()).to_pylist(), [1001000, 1001001])

    @parameterized.expand([("ms",), ("ns",)])
    def test_orc_directory_timestamp_partition(self, unit):
        orc = pytest.importorskip("pyarrow.orc")
        name = "default.partitioned_orc"
        schema = pa.schema([("id", pa.int64()), ("pt", pa.timestamp(unit))])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            schema, partition_keys=["pt"], options={"type": "format-table", "file.format": "orc"}), False)
        table = self.rest_catalog.get_table(name)
        directory = table.location() + "/pt=1500-01-01 00:00:00"
        table.file_io.check_or_mkdirs(directory)
        with table.file_io.new_output_stream(directory + "/part.orc") as stream:
            orc.write_table(pa.table({"id": [1]}), stream)
        builder = table.new_read_builder().with_projection(["pt"])
        reader = builder.new_read()
        splits = builder.new_scan().plan().splits()
        if unit == "ns":
            with self.assertRaisesRegex(ValueError, "Invalid timestamp partition"):
                reader.to_arrow(splits)
        else:
            expected = pa.table({"pt": pa.array(["1500-01-01 00:00:00"]).cast(pa.timestamp("ms"))})
            self._assert_format_read(table, expected, ["pt"])

    @parameterized.expand([("parquet",), ("orc",)])
    def test_columnar_required_field_with_null(self, file_format):
        if file_format == "orc":
            pytest.importorskip("pyarrow.orc")
        name = "default.required_id"
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            pa.schema([pa.field("id", pa.int64(), nullable=False)]),
            options={"type": "format-table", "file.format": file_format}), False)
        table = self.rest_catalog.get_table(name)
        self._write_arrow(table, pa.table({"id": [1, None]}))
        builder = table.new_read_builder()
        splits = builder.new_scan().plan().splits()
        reader = builder.new_read()
        for read in (reader.to_arrow, lambda s: list(reader.to_iterator(s))):
            with self.assertRaises(ValueError) as error:
                read(splits)
            self.assertIn("required column 'id'", str(error.exception))
            self.assertIn(splits[0].data_path(), str(error.exception))

    def test_format_table_write_rolls_by_row_count(self):
        pa_schema = pa.schema([("id", pa.int32())])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                "type": "format-table",
                "file.format": "parquet",
                "target-file-row-num": "2",
            },
        )
        table_name = "default.format_table_row_count_rolling"
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(pa.table({"id": [1, 2, 3, 4, 5]}))
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        self.assertEqual(3, len(splits))
        splits_by_name = {
            split.data_path().rsplit("/", 1)[-1]: split
            for split in splits
        }
        row_counts = [
            read_builder.new_read().to_arrow([
                splits_by_name[path.rsplit("/", 1)[-1]]
            ]).num_rows
            for path in commit_messages[0].written_paths
        ]
        self.assertEqual([2, 2, 1], row_counts)

    def test_format_table_overwrite_new_partition_in_multiple_batches(self):
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("pt", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=["pt"],
            options={
                "type": "format-table",
                "file.format": "parquet",
                "target-file-row-num": "2",
            },
        )
        table_name = "default.format_table_overwrite_new_partition_batches"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        write_builder = table.new_batch_write_builder().overwrite({"pt": 1})
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(pa.table({"id": [1, 2], "pt": [1, 1]}))
        table_write.write_arrow(pa.table({"id": [3, 4], "pt": [1, 1]}))
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        actual = read_builder.new_read().to_pandas(
            read_builder.new_scan().plan().splits()
        ).sort_values(by="id")
        self.assertEqual([1, 2, 3, 4], actual["id"].tolist())

    def test_format_table_text_read_write(self):
        pa_schema = pa.schema([("value", pa.string())])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"type": "format-table", "file.format": "text"},
        )
        table_name = "default.format_table_rw_text"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        self.assertIsInstance(table, FormatTable)
        self.assertEqual(table.format().value, "text")
        opts = table.options()
        self.assertIsInstance(opts, dict)
        self.assertEqual(opts.get("file.format"), "text")

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        df = pd.DataFrame({"value": ["hello", "world"]})
        table_write.write_pandas(df)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        table_read = read_builder.new_read()
        actual = table_read.to_pandas(splits).sort_values(by="value").reset_index(drop=True)
        expected = pd.DataFrame({"value": ["hello", "world"]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_format_table_text_read_write_with_nulls(self):
        pa_schema = pa.schema([("value", pa.string())])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"type": "format-table", "file.format": "text"},
        )
        table_name = "default.format_table_rw_text_nulls"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        df = pd.DataFrame({"value": ["hello", None, "world"]})
        table_write.write_pandas(df)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        table_read = read_builder.new_read()
        actual = table_read.to_pandas(splits)
        self.assertEqual(actual.shape[0], 3)
        # Nulls are written as empty string; read back as ""
        self.assertEqual(set(actual["value"].fillna("").astype(str)), {"", "hello", "world"})
        self.assertIn("", actual["value"].values)

    def test_format_table_text_partitioned_read_write(self):
        pa_schema = pa.schema([
            ("value", pa.string()),
            ("dt", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=["dt"],
            options={"type": "format-table", "file.format": "text"},
        )
        table_name = "default.format_table_rw_text_partitioned"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        self.assertIsInstance(table, FormatTable)
        self.assertEqual(table.format().value, "text")

        write_builder = table.new_batch_write_builder()
        tw = write_builder.new_write()
        tc = write_builder.new_commit()
        tw.write_pandas(pd.DataFrame({"value": ["a", "b"], "dt": [1, 1]}))
        tw.write_pandas(pd.DataFrame({"value": ["c"], "dt": [2]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        actual = read_builder.new_read().to_pandas(splits).sort_values(by=["dt", "value"]).reset_index(drop=True)
        self.assertEqual(actual.shape[0], 3)
        self.assertEqual(actual["value"].tolist(), ["a", "b", "c"])
        self.assertEqual(actual["dt"].tolist(), [1, 1, 2])

    def test_format_table_read_with_limit_to_iterator(self):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"type": "format-table", "file.format": "parquet"},
        )
        table_name = "default.format_table_limit_iterator"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        write_builder = table.new_batch_write_builder()
        tw = write_builder.new_write()
        tc = write_builder.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [1, 2, 3, 4], "b": [10, 20, 30, 40]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        splits = table.new_read_builder().new_scan().plan().splits()
        limit = 2
        read_builder = table.new_read_builder().with_limit(limit)
        table_read = read_builder.new_read()

        df = table_read.to_pandas(splits)
        self.assertEqual(len(df), limit, "to_pandas must respect with_limit(2)")

        batches = list(table_read.to_iterator(splits))
        self.assertEqual(len(batches), limit, "to_iterator must respect with_limit(2)")

    @parameterized.expand(_format_table_read_write_formats())
    def test_format_table_partitioned_overwrite(self, file_format):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
            ("c", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=["c"],
            options={"type": "format-table", "file.format": file_format},
        )
        table_name = f"default.format_table_partitioned_overwrite_{file_format}"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        write_builder = table.new_batch_write_builder()
        tw = write_builder.new_write()
        tc = write_builder.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [10, 10], "b": [10, 20], "c": [1, 1]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        tw = table.new_batch_write_builder().overwrite({"c": 1}).new_write()
        tc = table.new_batch_write_builder().overwrite({"c": 1}).new_commit()
        tw.write_pandas(pd.DataFrame({"a": [12, 12], "b": [100, 200], "c": [1, 1]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        actual = read_builder.new_read().to_pandas(splits).sort_values(by="b")
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual["b"].tolist(), [100, 200])

    def test_format_table_overwrite_only_specified_partition(self):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
            ("c", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=["c"],
            options={"type": "format-table", "file.format": "parquet"},
        )
        table_name = "default.format_table_overwrite_one_partition"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [10, 10], "b": [10, 20], "c": [1, 1]}))
        tw.write_pandas(pd.DataFrame({"a": [30, 30], "b": [30, 40], "c": [2, 2]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        tw = table.new_batch_write_builder().overwrite({"c": 1}).new_write()
        tc = table.new_batch_write_builder().overwrite({"c": 1}).new_commit()
        tw.write_pandas(pd.DataFrame({"a": [12, 12], "b": [100, 200], "c": [1, 1]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        actual = table.new_read_builder().new_read().to_pandas(
            table.new_read_builder().new_scan().plan().splits()
        ).sort_values(by=["c", "b"])
        self.assertEqual(len(actual), 4)
        self.assertEqual(actual["b"].tolist(), [100, 200, 30, 40])
        self.assertEqual(actual["c"].tolist(), [1, 1, 2, 2])
        c1 = actual[actual["c"] == 1]["b"].tolist()
        c2 = actual[actual["c"] == 2]["b"].tolist()
        self.assertEqual(c1, [100, 200], "partition c=1 must be overwritten")
        self.assertEqual(c2, [30, 40], "partition c=2 must be unchanged")

    def test_format_table_overwrite_multiple_batches_same_partition(self):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"type": "format-table", "file.format": "parquet"},
        )
        table_name = "default.format_table_overwrite_multi_batch"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [1, 2], "b": [10, 20]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        tw = wb.overwrite().new_write()
        tc = wb.overwrite().new_commit()
        tw.write_pandas(pd.DataFrame({"a": [3, 4], "b": [30, 40]}))
        tw.write_pandas(pd.DataFrame({"a": [5, 6], "b": [50, 60]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        actual = table.new_read_builder().new_read().to_pandas(
            table.new_read_builder().new_scan().plan().splits()
        ).sort_values(by="b")
        self.assertEqual(len(actual), 4, "overwrite + 2 write_pandas same partition must keep all 4 rows")
        self.assertEqual(actual["b"].tolist(), [30, 40, 50, 60])

    @parameterized.expand(_format_table_read_write_formats())
    def test_format_table_partitioned_read_write(self, file_format):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
            ("dt", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=["dt"],
            options={"type": "format-table", "file.format": file_format},
        )
        table_name = f"default.format_table_partitioned_rw_{file_format}"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        self.assertIsInstance(table, FormatTable)

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [1, 2], "b": [10, 20], "dt": [10, 10]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [3, 4], "b": [30, 40], "dt": [11, 11]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        splits_all = rb.new_scan().plan().splits()
        actual_all = rb.new_read().to_pandas(splits_all).sort_values(by="b")
        self.assertEqual(len(actual_all), 4)
        self.assertEqual(sorted(actual_all["b"].tolist()), [10, 20, 30, 40])

        rb_dt10 = table.new_read_builder().with_partition_filter({"dt": "10"})
        splits_dt10 = rb_dt10.new_scan().plan().splits()
        actual_dt10 = rb_dt10.new_read().to_pandas(splits_dt10).sort_values(by="b")
        self.assertEqual(len(actual_dt10), 2)
        self.assertEqual(actual_dt10["b"].tolist(), [10, 20])
        # Partition column must match schema type (int32), not string
        self.assertEqual(actual_dt10["dt"].tolist(), [10, 10])
        self.assertEqual(actual_all["dt"].tolist(), [10, 10, 11, 11])

    def test_format_table_partition_column_returns_schema_type(self):
        """Partition columns must be returned with schema type (e.g. int32), not always string."""
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
            ("dt", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=["dt"],
            options={"type": "format-table", "file.format": "parquet"},
        )
        table_name = "default.format_table_partition_schema_type"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [1, 2], "b": [10, 20], "dt": [1, 1]}))
        tw.write_pandas(pd.DataFrame({"a": [3, 4], "b": [30, 40], "dt": [2, 2]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder().with_partition_filter({"dt": "1"})
        splits = rb.new_scan().plan().splits()
        actual = rb.new_read().to_pandas(splits).sort_values(by="b")
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual["b"].tolist(), [10, 20])
        # Must be int list, not string list; fails if partition column is hardcoded as string
        self.assertEqual(actual["dt"].tolist(), [1, 1])
        self.assertTrue(
            actual["dt"].dtype in (pd.Int32Dtype(), "int32", "int64"),
            "dt must be int type per schema, not string",
        )

    def test_format_table_with_filter_extracts_partition_like_java(self):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
            ("dt", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=["dt"],
            options={"type": "format-table", "file.format": "parquet"},
        )
        table_name = "default.format_table_with_filter_assert"
        try:
            self.rest_catalog.drop_table(table_name, True)
        except Exception:
            pass
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [1, 2], "b": [10, 20], "dt": [10, 10]}))
        tw.write_pandas(pd.DataFrame({"a": [3, 4], "b": [30, 40], "dt": [11, 11]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        predicate_eq_dt10 = table.new_read_builder().new_predicate_builder().equal("dt", 10)
        splits_by_partition_filter = (
            table.new_read_builder().with_partition_filter({"dt": "10"}).new_scan().plan().splits()
        )
        splits_by_with_filter = (
            table.new_read_builder().with_filter(predicate_eq_dt10).new_scan().plan().splits()
        )
        self.assertEqual(
            len(splits_by_with_filter), len(splits_by_partition_filter),
            "with_filter(partition equality) must behave like with_partition_filter (Java-aligned)",
        )
        actual_from_filter = (
            table.new_read_builder().with_filter(predicate_eq_dt10).new_read().to_pandas(splits_by_with_filter)
        )
        self.assertEqual(len(actual_from_filter), 2)
        self.assertEqual(actual_from_filter["b"].tolist(), [10, 20])

        splits_partition_then_filter = (
            table.new_read_builder()
            .with_partition_filter({"dt": "10"})
            .with_filter(predicate_eq_dt10)
            .new_scan()
            .plan()
            .splits()
        )
        self.assertEqual(
            len(splits_partition_then_filter), len(splits_by_partition_filter),
            "with_filter must not overwrite a previously set partition filter",
        )
        actual = (
            table.new_read_builder()
            .with_partition_filter({"dt": "10"})
            .with_filter(predicate_eq_dt10)
            .new_read()
            .to_pandas(splits_partition_then_filter)
        )
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual["b"].tolist(), [10, 20])

        predicate_non_partition = table.new_read_builder().new_predicate_builder().equal("a", 1)
        splits_no_filter = table.new_read_builder().new_scan().plan().splits()
        splits_with_non_partition_predicate = (
            table.new_read_builder().with_filter(predicate_non_partition).new_scan().plan().splits()
        )
        self.assertEqual(
            len(splits_with_non_partition_predicate), len(splits_no_filter),
            "with_filter(non-partition predicate) must not change scan when no partition spec extracted",
        )

    @parameterized.expand(_format_table_read_write_formats())
    def test_format_table_full_overwrite(self, file_format):
        pa_schema = pa.schema([
            ("a", pa.int32()),
            ("b", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"type": "format-table", "file.format": file_format},
        )
        table_name = f"default.format_table_full_overwrite_{file_format}"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_pandas(pd.DataFrame({"a": [1, 2], "b": [10, 20]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        tw = wb.overwrite().new_write()
        tc = wb.overwrite().new_commit()
        tw.write_pandas(pd.DataFrame({"a": [3], "b": [30]}))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        actual = rb.new_read().to_pandas(splits)
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual["b"].tolist(), [30])

    @parameterized.expand(_format_table_read_write_formats())
    def test_format_table_split_read(self, file_format):
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                "type": "format-table",
                "file.format": file_format,
                "source.split.target-size": "54",
            },
        )
        table_name = f"default.format_table_split_read_{file_format}"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        size = 50
        for i in range(0, size, 10):
            batch = pd.DataFrame({
                "id": list(range(i, min(i + 10, size))),
                "name": [f"User{j}" for j in range(i, min(i + 10, size))],
                "score": [85.5 + (j % 15) for j in range(i, min(i + 10, size))],
            })
            wb = table.new_batch_write_builder()
            tw = wb.new_write()
            tc = wb.new_commit()
            tw.write_pandas(batch)
            tc.commit(tw.prepare_commit())
            tw.close()
            tc.close()

        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        actual = rb.new_read().to_pandas(splits).sort_values(by="id")
        self.assertEqual(len(actual), size)
        self.assertEqual(actual["id"].tolist(), list(range(size)))

    @parameterized.expand(_format_table_read_write_formats())
    def test_format_table_catalog(self, file_format):
        pa_schema = pa.schema([
            ("str", pa.string()),
            ("int", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"type": "format-table", "file.format": file_format},
        )
        table_name = f"default.format_table_catalog_{file_format}"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        self.assertIn(f"format_table_catalog_{file_format}", self.rest_catalog.list_tables("default"))
        table = self.rest_catalog.get_table(table_name)
        self.assertIsInstance(table, FormatTable)

        self.rest_catalog.drop_table(table_name, False)
        with self.assertRaises(TableNotExistException):
            self.rest_catalog.get_table(table_name)


if __name__ == "__main__":
    unittest.main()

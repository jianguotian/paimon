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
            options={"type": "format-table", "file.format": file_format},
        )
        self.rest_catalog.create_table(name, schema, False)

        self._write_arrow(self.rest_catalog.get_table(name), pa.table({"id": [1, 2]}))
        self.rest_catalog.alter_table(
            name, [SchemaChange.add_column("campaign", AtomicType("STRING"))])
        updated = pa.table({"id": [3], "campaign": ["spring"]}, schema=pa.schema([
            pa.field("id", pa.int64()), pa.field("campaign", pa.string(), nullable=False),
        ]))
        self._write_arrow(self.rest_catalog.get_table(name), updated)

        table = self.rest_catalog.get_table(name)
        builder = table.new_read_builder()
        splits = builder.new_scan().plan().splits()
        expected = pa.table({"id": [1, 2, 3], "campaign": [None, None, "spring"]})
        self.assertTrue(builder.new_read().to_arrow(splits).sort_by("id").equals(expected))

        reader = builder.with_projection(["campaign"]).new_read()
        batches = list(reader.to_iterator(splits))
        self.assertTrue(all(batch.schema.names == ["campaign"] for batch in batches))
        actual = pa.Table.from_batches(batches)
        self.assertCountEqual(actual.column("campaign").to_pylist(), [None, None, "spring"])

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

    @parameterized.expand([(1, "named"), (2, "positional")])
    def test_orc_positional_evolution(self, levels, nested_value):
        pytest.importorskip("pyarrow.orc")
        name = "default.positional"
        nested = pa.struct([("first", pa.string()), ("second", pa.string())])
        logical = pa.schema([("id", pa.int64()), ("pt", pa.string()),
                             ("label", nested), ("campaign", pa.string())])
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            logical, partition_keys=["pt"], options={
                "type": "format-table", "file.format": "orc", "orc.force.positional.evolution": "true",
                "orc.force.positional.evolution.level": str(levels)}), False)
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
        label = {"first": nested_value, "second": "positional" if levels == 1 else "named"}
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
        self.rest_catalog.create_table(name, Schema.from_pyarrow_schema(
            data.schema, options={"type": "format-table", "file.format": "parquet"}), False)
        table = self.rest_catalog.get_table(name)
        self._write_arrow(table, data)
        self._assert_format_read(table, data)

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

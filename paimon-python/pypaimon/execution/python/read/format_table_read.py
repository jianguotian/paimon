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

import json
import re
from typing import Any, Dict, Iterator, List, Optional

import pyarrow

from pypaimon.common.options.options_utils import OptionsUtils
from pypaimon.schema.data_types import AtomicType, MapType, MultisetType, PyarrowFieldParser, RowType
from pypaimon.table.format.format_data_split import FormatDataSplit
from pypaimon.table.format.format_table import FormatTable, Format


def _text_format_schema_column(table: FormatTable) -> Optional[str]:
    """TEXT format: single data column name from schema (non-partition)."""
    if table.format() != Format.TEXT or not table.fields:
        return None
    data_names = [
        f.name for f in table.fields if f.name not in table.partition_keys
    ]
    return (
        data_names[0]
        if data_names
        else (table.field_names[0] if table.field_names else None)
    )


def _columnar_read_schema(
    table: FormatTable, projection: Optional[List[str]]
) -> Optional[pyarrow.Schema]:
    fmt = table.format()
    if fmt not in (Format.PARQUET, Format.ORC):
        return None

    def read_type(data_type, partition=False):
        if isinstance(data_type, MultisetType):
            return pyarrow.map_(read_type(data_type.element), pyarrow.field("value", pyarrow.int32(), nullable=False))
        typ = PyarrowFieldParser.from_paimon_type(data_type, child_type=read_type)
        if fmt == Format.ORC and not partition and pyarrow.types.is_timestamp(typ):
            return pyarrow.timestamp("ns", tz=typ.tz)
        if fmt == Format.PARQUET and pyarrow.types.is_time(typ):
            # Python catalog schemas historically recorded every Arrow time unit as TIME(0).
            # All such Parquet values fit in time64[ns], even when read one split at a time.
            match = re.fullmatch(r"TIME\((\d+)\)", data_type.type.upper())
            precision = int(match.group(1)) if match else 0
            if precision == 0 or precision > 6:
                return pyarrow.time64("ns")
            if precision > 3:
                return pyarrow.time64("us")
        return typ

    # ORC timestamps from files use ns; directory timestamps retain the catalog range.
    return pyarrow.schema([
        PyarrowFieldParser.from_paimon_field(field, lambda typ: read_type(typ, field.name in table.partition_keys))
        for field in table.projected_fields(projection)
    ])


def _field_index(fields, name, path):
    indices = fields.get_all_field_indices(name)
    if len(indices) > 1:
        raise ValueError(f"Ambiguous column {name!r} in {path}")
    return indices[0] if indices else None


def _align_array(array, target, logical_type, name_key, positional_levels, fmt, path):
    """Resolve nested fields explicitly; older Arrow casts cannot reorder struct children."""
    source = array.type
    if source == target:
        return array

    def child(values, child_type, logical_child):
        return _align_array(values, child_type, logical_child, name_key, positional_levels - 1, fmt, path)

    types = pyarrow.types
    if types.is_struct(source) and types.is_struct(target):
        if isinstance(logical_type, AtomicType) and logical_type.type.upper() == "VARIANT":
            from pypaimon.data.variant_shredding import build_variant_schema
            from pypaimon.execution.python.read.variant_shredding import assemble_shredded_column
            if source.get_field_index("typed_value") >= 0 and source.get_field_index("metadata") >= 0:
                return assemble_shredded_column(array, build_variant_schema(source))
        names = pyarrow.schema([field.with_name(name_key(field.name)) for field in source])
        source_children = array.flatten()
        children = []
        for i, field in enumerate(target):
            index = i if positional_levels > 0 else _field_index(names, name_key(field.name), path)
            if index is None or index >= len(source):
                if not field.nullable and array.null_count != len(array):
                    raise ValueError(f"Missing required column {field.name!r} in {path}")
                values = pyarrow.nulls(len(array), field.type)
            else:
                logical_child = logical_type.fields[i].type if isinstance(logical_type, RowType) else None
                values = child(source_children[index], field.type, logical_child)
                if not field.nullable and values.null_count:
                    import pyarrow.compute as pc
                    if pc.any(pc.and_(array.is_valid(), values.is_null())).as_py():
                        raise ValueError(f"Null in required column {field.name!r} in {path}")
            children.append(values)
        return pyarrow.StructArray.from_arrays(children, fields=list(target), mask=array.is_null())

    if types.is_map(source) and types.is_map(target):
        keys = child(array.keys, target.key_type,
                     logical_type.key if isinstance(logical_type, MapType) else logical_type.element)
        items = child(array.items, target.item_type, logical_type.value if isinstance(logical_type, MapType) else None)
        if not target.item_field.nullable and items.null_count:
            entries = array.view(pyarrow.list_(source.field(0))).flatten()
            if entries.field(1).null_count:
                raise ValueError(f"Null in required map value in {path}")
        entries = pyarrow.StructArray.from_arrays([keys, items], fields=[target.key_field, target.item_field])
        return pyarrow.Array.from_buffers(target, len(array), array.buffers()[:source.num_buffers],
                                          offset=array.offset, children=[entries])

    list_types = (pyarrow.ListType, pyarrow.LargeListType, pyarrow.FixedSizeListType)
    if isinstance(source, list_types) and isinstance(target, list_types):
        values = child(array.values, target.value_type, logical_type.element)
        if not target.value_field.nullable and values.null_count and array.flatten().null_count:
            raise ValueError(f"Null in required list element in {path}")
        if types.is_large_list(source):
            layout = pyarrow.large_list(target.value_field)
        elif types.is_fixed_size_list(source):
            layout = pyarrow.list_(target.value_field, source.list_size)
        else:
            layout = pyarrow.list_(target.value_field)
        result = pyarrow.Array.from_buffers(layout, len(array), array.buffers()[:source.num_buffers],
                                            offset=array.offset, children=[values])
        return result.cast(target)

    if fmt == Format.ORC and types.is_int32(source) and types.is_time(target):
        # Paimon stores ORC TIME as milliseconds since midnight.
        array = array.cast(pyarrow.time32("ms"))
    return array.cast(target)


def _orc_positional_levels(table, file_names, path):
    if table.format() != Format.ORC:
        return 0
    options = table.options()
    if not OptionsUtils.convert_to_boolean(options.get("orc.force.positional.evolution", False)):
        # Hive writers predating HIVE-4243 only supplied synthetic field names.
        if not file_names or any(not re.fullmatch(r"_col[0-9]+", name) for name in file_names):
            return 0
        if file_names == [name for name in table.field_names if name not in table.partition_keys]:
            return 0
        tolerate = options.get("orc.tolerate.missing.schema",
                               options.get("hive.exec.orc.tolerate.missing.schema", True))
        if not OptionsUtils.convert_to_boolean(tolerate):
            raise ValueError(f"Missing ORC column names in {path}")
    return int(options.get("orc.force.positional.evolution.level", 1))


def _partition_scalar(value, field, default_partition):
    if value is None or value == default_partition:
        return pyarrow.scalar(None, type=field.type)
    text = str(value)
    if pyarrow.types.is_time(field.type) or pyarrow.types.is_timestamp(field.type):
        # datetime/time stringify milliseconds with six fractional digits. Arrow
        # accepts only the target precision, even when the extra digits are zero.
        text = re.sub(r"\.\d+", lambda match: match.group().rstrip("0").rstrip("."), text, count=1)
    if pyarrow.types.is_binary(field.type) or pyarrow.types.is_large_binary(field.type):
        # The Python writer uses str(bytes) as the directory component. External
        # writers can supply plain UTF-8 instead.
        if text.startswith(("b'", 'b"')):
            import ast
            try:
                binary = ast.literal_eval(text)
            except (ValueError, SyntaxError):
                binary = None
            if isinstance(binary, bytes) and repr(binary) == text:
                return pyarrow.scalar(binary, type=field.type)
    if pyarrow.types.is_time(field.type):
        # Arrow parses ISO timestamps, but has no string-to-time cast kernel.
        return pyarrow.scalar("1970-01-01T" + text).cast(pyarrow.timestamp(field.type.unit)).cast(field.type)
    scalar = pyarrow.scalar(text).cast(field.type)
    if pyarrow.types.is_timestamp(field.type) and field.type.unit == "ns":
        # Arrow's direct string-to-ns cast can silently wrap outside the int64 range.
        from pandas import Timestamp
        try:
            ticks = Timestamp(text).value
        except (ValueError, OverflowError) as error:
            raise ValueError(f"Invalid timestamp partition {field.name}={text!r}") from error
        if ticks != scalar.cast(pyarrow.int64()).as_py():
            raise ValueError(f"Invalid timestamp partition {field.name}={text!r}")
    return scalar


def _project_metadata(file_schema, column_names):
    metadata = file_schema.metadata
    if not metadata or b"pandas" not in metadata:
        return metadata
    pandas = file_schema.pandas_metadata
    columns = []
    for entry in pandas["columns"]:
        name = entry.get("field_name", entry["name"])
        if name in column_names:
            projected = column_names[name]
            columns.append(entry if name == projected else dict(entry, field_name=projected, name=projected))
    pandas["columns"] = columns
    pandas["index_columns"] = [column_names.get(idx, idx) if isinstance(idx, str) else idx
                               for idx in pandas["index_columns"] if not isinstance(idx, str) or idx in column_names]
    return {**metadata, b"pandas": json.dumps(pandas).encode()}


def _concat_columnar_tables(tables):
    out = pyarrow.concat_tables(tables)
    if len(tables) == 1:
        return out
    # concat_tables takes metadata only from the first file. An evolved column's
    # nullable Pandas dtype may first appear in a later file.
    pandas = None
    columns = {}
    for table in tables:
        meta = table.schema.pandas_metadata
        if meta is None:
            continue
        if pandas is None:
            pandas = meta
        for entry in meta["columns"]:
            name = entry.get("field_name", entry["name"])
            if name not in columns or entry["numpy_type"] in ("Int64", "UInt64"):
                columns[name] = entry
    if pandas is not None:
        pandas["columns"] = list(columns.values())
        metadata = dict(out.schema.metadata or {})
        metadata[b"pandas"] = json.dumps(pandas).encode()
        out = out.replace_schema_metadata(metadata)
    return out


def _align_columnar_table(tbl, schema, table, split):
    positional_levels = _orc_positional_levels(table, tbl.column_names, split.data_path())

    def name_key(name):
        return name if table.case_sensitive else name.lower()

    file_names = pyarrow.schema([field.with_name(name_key(field.name)) for field in tbl.schema])
    positions = {}
    partition_keys = {name_key(k) for k in table.partition_keys}
    if positional_levels > 0:
        # ORC ordinals refer to the full data schema, before projection or partition injection.
        data_names = (name for name in table.field_names if name_key(name) not in partition_keys)
        data_indices = (i for i, name in enumerate(file_names.names) if name not in partition_keys)
        positions = dict(zip(data_names, data_indices))

    arrays = []
    column_names = {}
    logical_types = {field.name: field.type for field in table.fields}
    partition = split.partition or {}
    default_partition = table.options().get("partition.default-name", "__DEFAULT_PARTITION__")
    for field in schema:
        key = name_key(field.name)
        if field.name in partition:
            # Scan pruning and returned partition values must use the same directory.
            scalar = _partition_scalar(partition[field.name], field, default_partition)
            column = pyarrow.repeat(scalar, tbl.num_rows)
            indices = file_names.get_all_field_indices(key)
            if len(indices) == 1 and tbl.schema.field(indices[0]).type == field.type:
                column_names[tbl.schema.field(indices[0]).name] = field.name
        else:
            if positional_levels > 0 and key not in partition_keys:
                index = positions.get(field.name)
            else:
                index = _field_index(file_names, key, split.data_path())
            if index is not None:
                column = tbl.column(index)
                if column.type == field.type:
                    column_names[tbl.schema.field(index).name] = field.name
                else:
                    chunks = [_align_array(chunk, field.type, logical_types[field.name], name_key,
                                           positional_levels - 1, table.format(), split.data_path())
                              for chunk in column.chunks]
                    column = pyarrow.chunked_array(chunks, type=field.type)
            elif field.nullable:
                column = pyarrow.nulls(tbl.num_rows, type=field.type)
            else:
                raise ValueError(f"Missing required column {field.name!r} in {split.data_path()}")
        if not field.nullable and column.null_count:
            raise ValueError(f"Null in required column {field.name!r} in {split.data_path()}")
        arrays.append(column)
    return pyarrow.Table.from_arrays(arrays, schema=schema.with_metadata(_project_metadata(tbl.schema, column_names)))


def _read_file_to_arrow(
    table: FormatTable,
    split: FormatDataSplit,
    read_fields: Optional[List[str]],
    partition_key_types: Optional[Dict[str, pyarrow.DataType]] = None,
    columnar_schema: Optional[pyarrow.Schema] = None,
    text_column_name: Optional[str] = None,
    text_line_delimiter: str = "\n",
) -> pyarrow.Table:
    path = split.data_path()
    fmt = table.format()
    partition_spec = split.partition
    csv_read_options = None
    if fmt == Format.CSV:
        import pyarrow.csv as csv
        csv_read_options = csv.ReadOptions(block_size=1 << 20)
    try:
        with table.file_io.new_input_stream(path) as stream:
            chunks = []
            while True:
                chunk = stream.read()
                if not chunk:
                    break
                chunks.append(
                    chunk if isinstance(chunk, bytes) else bytes(chunk)
                )
            data = b"".join(chunks)
    except Exception as e:
        raise RuntimeError(f"Failed to read {path}") from e

    if not data and fmt != Format.PARQUET:
        # Java's ORC reader reads a zero-byte file as empty, as Hive writes them.
        return pyarrow.table({})

    if fmt == Format.PARQUET:
        import io
        import pyarrow.parquet as pq
        data = (
            bytes(data) if not isinstance(data, bytes) else data
        )
        try:
            tbl = pq.read_table(io.BytesIO(data))
        except (pyarrow.ArrowException, OSError) as e:
            raise RuntimeError(f"Failed to read {path}") from e
    elif fmt == Format.CSV:
        import pyarrow.csv as csv
        try:
            tbl = csv.read_csv(
                pyarrow.BufferReader(data),
                read_options=csv_read_options,
            )
        except (pyarrow.ArrowException, OSError) as e:
            raise RuntimeError(f"Failed to read {path}") from e
    elif fmt == Format.JSON:
        import json
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        records = []
        for line in text.strip().split("\n"):
            line = line.strip()
            if line:
                records.append(json.loads(line))
        if not records:
            return pyarrow.table({})
        tbl = pyarrow.Table.from_pylist(records)
    elif fmt == Format.ORC:
        import io
        import pyarrow.orc as orc
        data = bytes(data) if not isinstance(data, bytes) else data
        try:
            tbl = orc.read_table(io.BytesIO(data))
        except (pyarrow.ArrowException, OSError) as e:
            raise RuntimeError(f"Failed to read {path}") from e
    elif fmt == Format.TEXT:
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        lines = (
            text.rstrip(text_line_delimiter).split(text_line_delimiter)
            if text
            else []
        )
        if not lines:
            return pyarrow.table({})
        part_keys = set(partition_spec.keys()) if partition_spec else set()
        col_name = text_column_name if text_column_name else "value"
        if read_fields:
            for f in read_fields:
                if f not in part_keys:
                    col_name = f
                    break
        tbl = pyarrow.table({col_name: lines})
    else:
        raise ValueError(f"Format {fmt} read not implemented in Python")

    if columnar_schema is not None:
        return _align_columnar_table(tbl, columnar_schema, table, split)

    if partition_spec:
        for k, v in partition_spec.items():
            if k in tbl.column_names:
                continue
            pa_type = (
                partition_key_types.get(k, pyarrow.string())
                if partition_key_types
                else pyarrow.string()
            )
            arr = pyarrow.array([v] * tbl.num_rows, type=pyarrow.string())
            if pa_type != pyarrow.string():
                arr = arr.cast(pa_type)
            tbl = tbl.append_column(k, arr)

    if read_fields and tbl.num_columns > 0:
        existing = [c for c in read_fields if c in tbl.column_names]
        if existing:
            tbl = tbl.select(existing)
    return tbl


def _partition_key_types(
    table: FormatTable,
) -> Optional[Dict[str, pyarrow.DataType]]:
    """Build partition column name -> PyArrow type from table schema."""
    if not table.partition_keys:
        return None
    result = {}
    for f in table.fields:
        if f.name in table.partition_keys:
            pa_field = PyarrowFieldParser.from_paimon_field(f)
            result[f.name] = pa_field.type
    return result if result else None


def to_arrow(
    self,
    splits: List[FormatDataSplit],
) -> pyarrow.Table:
    read_fields = self.projection
    fmt = self.table.format()
    columnar_schema = _columnar_read_schema(self.table, self.projection)
    partition_key_types = _partition_key_types(self.table)
    text_col = (
        _text_format_schema_column(self.table)
        if fmt == Format.TEXT
        else None
    )
    text_delim = (
        self.table.options().get("text.line-delimiter", "\n")
        if fmt == Format.TEXT
        else "\n"
    )
    tables = []
    nrows = 0
    for split in splits:
        t = _read_file_to_arrow(
            self.table,
            split,
            read_fields,
            partition_key_types,
            columnar_schema=columnar_schema,
            text_column_name=text_col,
            text_line_delimiter=text_delim,
        )
        if t.num_rows > 0:
            tables.append(t)
            nrows += t.num_rows
            if self.limit is not None and nrows >= self.limit:
                if nrows > self.limit:
                    excess = nrows - self.limit
                    last = tables[-1]
                    tables[-1] = last.slice(0, last.num_rows - excess)
                break
    if not tables:
        if columnar_schema is not None:
            return columnar_schema.empty_table()
        fields = self.table.fields
        if read_fields:
            fields = [
                f for f in self.table.fields if f.name in read_fields
            ]
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        return pyarrow.Table.from_pydict(
            {n: [] for n in schema.names},
            schema=schema,
        )
    out = _concat_columnar_tables(tables) if columnar_schema is not None else pyarrow.concat_tables(tables)
    if self.limit is not None and out.num_rows > self.limit:
        out = out.slice(0, self.limit)
    return out


def to_iterator(
    self,
    splits: List[FormatDataSplit],
) -> Iterator[Any]:
    columnar_schema = _columnar_read_schema(self.table, self.projection)
    partition_key_types = _partition_key_types(self.table)
    fmt = self.table.format()
    text_col = (
        _text_format_schema_column(self.table)
        if fmt == Format.TEXT
        else None
    )
    text_delim = (
        self.table.options().get("text.line-delimiter", "\n")
        if fmt == Format.TEXT
        else "\n"
    )
    n_yielded = 0
    for split in splits:
        if self.limit is not None and n_yielded >= self.limit:
            break
        t = _read_file_to_arrow(
            self.table,
            split,
            self.projection,
            partition_key_types,
            columnar_schema=columnar_schema,
            text_column_name=text_col,
            text_line_delimiter=text_delim,
        )
        for batch in t.to_batches():
            for i in range(batch.num_rows):
                if self.limit is not None and n_yielded >= self.limit:
                    return
                yield batch.slice(i, 1)
                n_yielded += 1

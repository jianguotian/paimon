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

import re
from dataclasses import replace
from typing import Dict, List, Optional
from urllib.parse import unquote

import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO
from pypaimon.read.plan import Plan
from pypaimon.table.format.format_data_split import FormatDataSplit
from pypaimon.table.format.format_table import Format


def _is_data_file_name(name: str) -> bool:
    if name is None:
        return False
    return not name.startswith(".") and not name.startswith("_")


def _is_reserved_dir_name(name: str) -> bool:
    if not name:
        return True
    if name.startswith(".") or name.startswith("_"):
        return True
    if name.lower() in ("schema", "_schema"):
        return True
    return False


def _list_data_files_recursive(
    file_io: FileIO,
    path: str,
    partition_keys: List[str],
    partition_only_value: bool,
    decode_partitions: bool,
    rel_path_parts: Optional[List[str]] = None,
) -> List[FormatDataSplit]:
    splits: List[FormatDataSplit] = []
    rel_path_parts = rel_path_parts or []
    try:
        infos = file_io.list_status(path)
    except FileNotFoundError:
        # The directory vanished after its parent listed it.
        return splits
    if not infos:
        return splits
    path_rstrip = path.rstrip("/")
    for info in infos:
        name = info.path.split("/")[-1] if "/" in info.path else info.path
        full_path = f"{path_rstrip}/{name}" if path_rstrip else name
        if info.path.startswith("/") or info.path.startswith("file:"):
            full_path = info.path
        if info.type == pafs.FileType.Directory:
            if _is_reserved_dir_name(name):
                continue
            part_value = name
            if not partition_only_value and "=" in name:
                part_value = name.split("=", 1)[1]
            child_parts = rel_path_parts + [part_value]
            if len(child_parts) <= len(partition_keys):
                sub_splits = _list_data_files_recursive(
                    file_io,
                    full_path,
                    partition_keys,
                    partition_only_value,
                    decode_partitions,
                    child_parts,
                )
                splits.extend(sub_splits)
        elif info.type == pafs.FileType.File and _is_data_file_name(name):
            size = getattr(info, "size", None) or 0
            part_spec: Optional[Dict[str, str]] = None
            raw = {}
            if partition_keys and len(rel_path_parts) >= len(partition_keys):
                part_spec = dict(
                    zip(
                        partition_keys,
                        rel_path_parts[: len(partition_keys)],
                    )
                )
                if decode_partitions:
                    for key, value in part_spec.items():
                        # Hive escapes individual characters, not UTF-8 byte sequences.
                        decoded = unquote(value, encoding="latin-1")
                        if decoded != value:
                            raw[key] = value
                            part_spec[key] = decoded
            splits.append(
                FormatDataSplit(
                    file_path=full_path,
                    file_size=size,
                    partition=part_spec,
                    raw_partition=raw or None,
                )
            )
    return splits


def _clock_with_nanoseconds(value):
    match = re.fullmatch(r"(\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?", str(value))
    if match:
        return match.group(1) + "." + (match.group(2) or "").ljust(9, "0")
    return None


def _filter_splits(table, splits, partition_filter):
    import pyarrow as pa
    from pypaimon.execution.python.read.format_table_read import _columnar_read_schema, _legacy_time_directory

    keys = [key for key in partition_filter if key in table.partition_keys]
    if len(keys) != len(partition_filter):
        return
    fields = {f.name: f for f in (_columnar_read_schema(table, keys) or [])}
    default = table.options().get("partition.default-name", "__DEFAULT_PARTITION__")
    ns_filters = {k: _clock_with_nanoseconds(v) for k, v in partition_filter.items()
                  if k in fields and fields[k].type == pa.time64("ns") and v != default}

    for split in splits:
        if not split.partition:
            continue
        residual = {}
        for key, requested in partition_filter.items():
            value = split.partition[key]
            if ns_filters.get(key) is not None and value != default:
                clock = _clock_with_nanoseconds(value)
                if _legacy_time_directory(value):
                    if clock[:-3] != ns_filters[key][:-3]:
                        break
                    residual[key] = requested
                elif clock != ns_filters[key]:
                    break
            elif split.raw_partition and key in split.raw_partition and key in fields and (
                    pa.types.is_string(fields[key].type) or pa.types.is_binary(fields[key].type)):
                if str(requested) not in (value, split.raw_partition[key]):
                    break
                residual[key] = requested
            elif str(value) != str(requested):
                break
        else:
            yield replace(split, partition_filter=residual) if residual else split


def plan(self) -> Plan:
    partition_only_value = self.table.options().get(
        "format-table.partition-path-only-value", "false"
    ).lower() == "true"
    splits = _list_data_files_recursive(
        self.table.file_io,
        self.table.location(),
        self.table.partition_keys,
        partition_only_value,
        self.table.format() in (Format.PARQUET, Format.ORC),
    )
    if self.partition_filter:
        splits = list(_filter_splits(self.table, splits, self.partition_filter))
    return Plan(_splits=splits)

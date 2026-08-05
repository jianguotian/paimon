/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.mosaic;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.reader.ArrowVectorizedRecordIterator;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Mosaic coverage for projected raw reads of partitioned append tables. */
class PartitionedMosaicRawFileSplitReadTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void unpartitionedMosaicKeepsLegacyProjectedMapping() throws Exception {
        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", false);
        Identifier identifier = Identifier.create("default", "unpartitioned");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .option("bucket", "1")
                        .option("bucket-key", "k")
                        .option("write-only", "true")
                        .option("file.format", "mosaic")
                        .build(),
                false);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        try (TableWriteImpl<?> write = table.newWrite("test");
                TableCommitImpl commit = table.newCommit("test")) {
            write.write(GenericRow.of(1, BinaryString.fromString("v1")));
            commit.commit(0, write.prepareCommit(false, 0));
        }

        table = (FileStoreTable) catalog.getTable(identifier);
        DataSplit split = table.newSnapshotReader().read().dataSplits().get(0);
        RawFileSplitRead read = (RawFileSplitRead) table.store().newRead();
        read.withReadType(table.rowType().project("k"));

        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            assertThat(batch).isNotInstanceOf(ArrowVectorizedRecordIterator.class);
            try {
                InternalRow row = batch.next();
                assertThat(row).isNotNull();
                assertThat(row.getFieldCount()).isEqualTo(1);
                assertThat(row.getInt(0)).isEqualTo(1);
                assertThat(batch.next()).isNull();
            } finally {
                batch.releaseBatch();
            }
        }
    }

    @Test
    void projectedPhysicalReadKeepsArrowBundleAndNormalReadInjectsPartitions() throws Exception {
        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", false);
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .column("hh", DataTypes.STRING())
                        .partitionKeys("dt", "hh")
                        .option("bucket", "1")
                        .option("bucket-key", "k")
                        .option("write-only", "true")
                        .option("file.format", "mosaic")
                        .build(),
                false);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        try (TableWriteImpl<?> write = table.newWrite("test");
                TableCommitImpl commit = table.newCommit("test")) {
            write.write(
                    GenericRow.of(
                            1,
                            BinaryString.fromString("v1"),
                            BinaryString.fromString("2026-08-04"),
                            BinaryString.fromString("13")));
            commit.commit(0, write.prepareCommit(false, 0));
        }

        table = (FileStoreTable) catalog.getTable(identifier);
        DataSplit split = table.newSnapshotReader().read().dataSplits().get(0);
        RowType physicalType = table.rowType().project("k", "v");
        RawFileSplitRead read = (RawFileSplitRead) table.store().newRead();
        read.withReadType(physicalType);

        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isInstanceOf(ArrowVectorizedRecordIterator.class);
            try {
                ArrowBundleRecords bundle = ((ArrowVectorizedRecordIterator) batch).arrowBundle();
                assertThat(bundle.getRowType()).isEqualTo(physicalType);
                assertThat(bundle.getVectorSchemaRoot().getSchema().getFields())
                        .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                        .containsExactly("k", "v");
            } finally {
                batch.releaseBatch();
            }
        }

        RowType valueOnlyType = table.rowType().project("v");
        read.withReadType(valueOnlyType);
        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isInstanceOf(ArrowVectorizedRecordIterator.class);
            try {
                ArrowBundleRecords bundle = ((ArrowVectorizedRecordIterator) batch).arrowBundle();
                assertThat(bundle.getRowType()).isEqualTo(valueOnlyType);
                assertThat(bundle.getVectorSchemaRoot().getSchema().getFields())
                        .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                        .containsExactly("v");
                assertThat(batch.next().getString(0).toString()).isEqualTo("v1");
            } finally {
                batch.releaseBatch();
            }
        }

        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(table.newRead().createReader(split))) {
            assertThat(rows.hasNext()).isTrue();
            InternalRow row = rows.next();
            assertThat(row.getInt(0)).isEqualTo(1);
            assertThat(row.getString(1).toString()).isEqualTo("v1");
            assertThat(row.getString(2).toString()).isEqualTo("2026-08-04");
            assertThat(row.getString(3).toString()).isEqualTo("13");
            assertThat(rows.hasNext()).isFalse();
        }
    }
}

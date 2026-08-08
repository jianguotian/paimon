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
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.reader.ArrowVectorizedRecordIterator;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.io.FileWriterContext;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.io.SimpleStatsProducer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Integration tests for Mosaic reader and writer. */
class MosaicReaderWriterTest {

    @TempDir java.nio.file.Path tempDir;

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @Test
    void testWriteAndRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        writeRows(
                rowType,
                path,
                GenericRow.of(1, BinaryString.fromString("hello")),
                GenericRow.of(2, BinaryString.fromString("world")));

        List<InternalRow> result = readAll(rowType, rowType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("hello");
        assertThat(result.get(1).getInt(0)).isEqualTo(2);
        assertThat(result.get(1).getString(1).toString()).isEqualTo("world");
    }

    @Test
    void testExactSchemaReadExposesArrowBundle() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();
        Path path = newPath();
        writeRows(
                rowType,
                path,
                GenericRow.of(1, BinaryString.fromString("hello")),
                GenericRow.of(2, BinaryString.fromString("world")));

        MosaicFileFormat format = createFormat();
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        LocalFileIO fileIO = new LocalFileIO();
        try (RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)))) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isInstanceOf(ArrowVectorizedRecordIterator.class);
            ArrowVectorizedRecordIterator arrowBatch = (ArrowVectorizedRecordIterator) batch;
            assertThat(arrowBatch.arrowBundle().rowCount()).isEqualTo(2);
            assertThat(arrowBatch.arrowBundle().getVectorSchemaRoot().getSchema().getFields())
                    .hasSize(2);
            batch.releaseBatch();
        }
    }

    @Test
    void testCrossRootDataFileDirectArrowRewriteCombinesRowGroups() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();
        Path sourcePath = newPath();
        Path targetPath = newPath();
        LocalFileIO fileIO = new LocalFileIO();

        MosaicFileFormat sourceFormat =
                new MosaicFileFormat(
                        new FileFormatFactory.FormatContext(
                                new Options(),
                                1024,
                                2,
                                MemorySize.VALUE_128_MB,
                                1,
                                MemorySize.parse("1 b")));
        FormatWriter sourceWriter =
                sourceFormat
                        .createWriterFactory(rowType)
                        .create(fileIO.newOutputStream(sourcePath, false), "zstd");
        for (int i = 0; i < 6; i++) {
            sourceWriter.addElement(GenericRow.of(i, BinaryString.fromString("value-" + i)));
        }
        sourceWriter.close();

        try (RootAllocator allocator = new RootAllocator();
                MosaicInputFileAdapter input = new MosaicInputFileAdapter(fileIO, sourcePath);
                MosaicReader sourceNativeReader =
                        MosaicReader.open(input, fileIO.getFileSize(sourcePath), allocator)) {
            assertThat(sourceNativeReader.numRowGroups()).isGreaterThan(1);
        }

        MosaicFileFormat targetFormat = createFormat();
        CapturingMosaicWriterFactory targetWriterFactory =
                new CapturingMosaicWriterFactory(targetFormat.createWriterFactory(rowType));
        LongCounter sequenceCounter = new LongCounter(5);
        RowDataFileWriter targetWriter =
                new RowDataFileWriter(
                        fileIO,
                        new FileWriterContext(targetWriterFactory, disabledStatsProducer(), "zstd"),
                        targetPath,
                        rowType,
                        1,
                        () -> sequenceCounter,
                        new FileIndexOptions(),
                        FileSource.COMPACT,
                        false,
                        false,
                        false,
                        null);
        FormatReaderFactory sourceReaderFactory =
                sourceFormat.createReaderFactory(rowType, rowType, null);
        FormatReaderContext sourceContext =
                new FormatReaderContext(fileIO, sourcePath, fileIO.getFileSize(sourcePath));
        // Reader and writer factories own independent RootAllocators. Direct writing the borrowed
        // batch therefore exercises paimon-mosaic's cross-root contract.
        try (DataFileRecordReader sourceReader =
                        new DataFileRecordReader(
                                rowType,
                                sourceReaderFactory,
                                sourceContext,
                                false,
                                false,
                                null,
                                null,
                                null,
                                false,
                                null,
                                0,
                                Collections.emptyMap());
                RowDataFileWriter ignored = targetWriter) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = sourceReader.readBatch()) != null) {
                try {
                    assertThat(batch).isInstanceOf(ArrowVectorizedRecordIterator.class);
                    ArrowBundleRecords bundle =
                            ((ArrowVectorizedRecordIterator) batch).arrowBundle();
                    assertThat(bundle).isInstanceOf(MosaicArrowBundleRecords.class);
                    assertThat(bundle.hasIdentityMapping()).isTrue();
                    targetWriter.writeBundle(bundle);
                } finally {
                    batch.releaseBatch();
                }
            }
        }

        DataFileMeta targetFile = targetWriter.result();
        MosaicRecordsWriter mosaicWriter = targetWriterFactory.writer();
        assertThat(targetWriter.recordCount()).isEqualTo(6);
        assertThat(sequenceCounter.getValue()).isEqualTo(11);
        assertThat(targetFile.rowCount()).isEqualTo(6);
        assertThat(targetFile.minSequenceNumber()).isEqualTo(5);
        assertThat(targetFile.maxSequenceNumber()).isEqualTo(10);
        assertThat(mosaicWriter.directArrowRows()).isEqualTo(6);
        assertThat(mosaicWriter.schemaCompatibilityFallbackRows()).isZero();

        List<InternalRow> result = readAll(rowType, rowType, targetPath, null);
        assertThat(result).hasSize(6);
        for (int i = 0; i < result.size(); i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getString(1).toString()).isEqualTo("value-" + i);
        }

        try (RootAllocator allocator = new RootAllocator();
                MosaicInputFileAdapter input = new MosaicInputFileAdapter(fileIO, targetPath);
                MosaicReader targetReader =
                        MosaicReader.open(input, fileIO.getFileSize(targetPath), allocator)) {
            assertThat(targetReader.numRowGroups()).isEqualTo(1);
        }
    }

    @Test
    void testExternalRootArrowBundleUsesDirectWrite() throws IOException {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();
        Path targetPath = newPath();
        LocalFileIO fileIO = new LocalFileIO();
        MosaicFileFormat targetFormat = createFormat();

        try (FormatWriter formatWriter =
                        targetFormat
                                .createWriterFactory(rowType)
                                .create(fileIO.newOutputStream(targetPath, false), "zstd");
                RootAllocator sourceAllocator = new RootAllocator();
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            MosaicRecordsWriter targetWriter = (MosaicRecordsWriter) formatWriter;
            IntVector ids = (IntVector) root.getVector("id");
            ids.allocateNew(1);
            ids.setSafe(0, 42);
            ids.setValueCount(1);
            root.setRowCount(1);

            targetWriter.writeBundle(new ArrowBundleRecords(root, rowType, true));

            assertThat(targetWriter.directArrowRows()).isEqualTo(1);
            assertThat(targetWriter.schemaCompatibilityFallbackRows()).isZero();
        }

        List<InternalRow> result = readAll(rowType, rowType, targetPath, null);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getInt(0)).isEqualTo(42);
    }

    @Test
    void testRenamedColumnRewriteFallsBackWithoutDataLoss() throws IOException {
        RowType sourceType =
                new RowType(
                        Collections.singletonList(new DataField(7, "old_name", DataTypes.INT())));
        RowType targetType =
                new RowType(
                        Collections.singletonList(new DataField(7, "new_name", DataTypes.INT())));
        Path sourcePath = newPath();
        Path targetPath = newPath();
        LocalFileIO fileIO = new LocalFileIO();
        writeRows(sourceType, sourcePath, GenericRow.of(42));

        MosaicFileFormat format = createFormat();
        FormatReaderFactory sourceReaderFactory =
                format.createReaderFactory(sourceType, sourceType, null);
        FormatReaderContext sourceContext =
                new FormatReaderContext(fileIO, sourcePath, fileIO.getFileSize(sourcePath));
        try (DataFileRecordReader sourceReader =
                        new DataFileRecordReader(
                                targetType,
                                sourceReaderFactory,
                                sourceContext,
                                false,
                                false,
                                null,
                                null,
                                null,
                                false,
                                null,
                                0,
                                Collections.emptyMap());
                FormatWriter formatWriter =
                        format.createWriterFactory(targetType)
                                .create(fileIO.newOutputStream(targetPath, false), "zstd")) {
            MosaicRecordsWriter targetWriter = (MosaicRecordsWriter) formatWriter;
            RecordReader.RecordIterator<InternalRow> batch = sourceReader.readBatch();
            assertThat(batch).isInstanceOf(ArrowVectorizedRecordIterator.class);
            try {
                targetWriter.writeBundle(((ArrowVectorizedRecordIterator) batch).arrowBundle());
            } finally {
                batch.releaseBatch();
            }
            assertThat(sourceReader.readBatch()).isNull();
            assertThat(targetWriter.directArrowRows()).isZero();
            assertThat(targetWriter.schemaCompatibilityFallbackRows()).isEqualTo(1);
        }

        List<InternalRow> result = readAll(targetType, targetType, targetPath, null);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getInt(0)).isEqualTo(42);
    }

    @Test
    void testNotNullToNullableRewriteFallsBackWithoutDataLoss() throws IOException {
        RowType sourceType =
                new RowType(
                        Collections.singletonList(
                                new DataField(7, "value", DataTypes.INT().notNull())));
        RowType targetType =
                new RowType(Collections.singletonList(new DataField(7, "value", DataTypes.INT())));
        Path sourcePath = newPath();
        Path targetPath = newPath();
        LocalFileIO fileIO = new LocalFileIO();
        writeRows(sourceType, sourcePath, GenericRow.of(41), GenericRow.of(42));

        IndexCastMapping mapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        targetType.getFields(), sourceType.getFields());
        assertThat(mapping.getIndexMapping()).isNull();
        assertThat(mapping.getCastMapping()).isNull();

        MosaicFileFormat format = createFormat();
        FormatReaderFactory sourceReaderFactory =
                format.createReaderFactory(sourceType, sourceType, null);
        FormatReaderContext sourceContext =
                new FormatReaderContext(fileIO, sourcePath, fileIO.getFileSize(sourcePath));
        CapturingMosaicWriterFactory targetWriterFactory =
                new CapturingMosaicWriterFactory(format.createWriterFactory(targetType));
        LongCounter sequenceCounter = new LongCounter(5);
        RowDataFileWriter targetWriter =
                new RowDataFileWriter(
                        fileIO,
                        new FileWriterContext(targetWriterFactory, disabledStatsProducer(), "zstd"),
                        targetPath,
                        targetType,
                        1,
                        () -> sequenceCounter,
                        new FileIndexOptions(),
                        FileSource.COMPACT,
                        false,
                        false,
                        false,
                        null);

        try (DataFileRecordReader sourceReader =
                        new DataFileRecordReader(
                                targetType,
                                sourceReaderFactory,
                                sourceContext,
                                false,
                                false,
                                mapping.getIndexMapping(),
                                mapping.getCastMapping(),
                                null,
                                false,
                                null,
                                0,
                                Collections.emptyMap());
                RowDataFileWriter ignored = targetWriter) {
            RecordReader.RecordIterator<InternalRow> batch = sourceReader.readBatch();
            assertThat(batch).isInstanceOf(ArrowVectorizedRecordIterator.class);
            try {
                targetWriter.writeBundle(((ArrowVectorizedRecordIterator) batch).arrowBundle());
            } finally {
                batch.releaseBatch();
            }
            assertThat(sourceReader.readBatch()).isNull();
        }

        DataFileMeta targetFile = targetWriter.result();
        MosaicRecordsWriter mosaicWriter = targetWriterFactory.writer();
        assertThat(targetWriter.recordCount()).isEqualTo(2);
        assertThat(sequenceCounter.getValue()).isEqualTo(7);
        assertThat(targetFile.rowCount()).isEqualTo(2);
        assertThat(targetFile.minSequenceNumber()).isEqualTo(5);
        assertThat(targetFile.maxSequenceNumber()).isEqualTo(6);
        assertThat(mosaicWriter.directArrowRows()).isZero();
        assertThat(mosaicWriter.schemaCompatibilityFallbackRows()).isEqualTo(2);

        List<InternalRow> result = readAll(targetType, targetType, targetPath, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getInt(0)).isEqualTo(41);
        assertThat(result.get(1).getInt(0)).isEqualTo(42);
    }

    @Test
    void testNullValues() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        writeRows(
                rowType,
                path,
                GenericRow.of(1, null),
                GenericRow.of(null, BinaryString.fromString("test")),
                GenericRow.of(null, null));

        List<InternalRow> result = readAll(rowType, rowType, path, null);
        assertThat(result).hasSize(3);
        assertThat(result.get(0).isNullAt(1)).isTrue();
        assertThat(result.get(1).isNullAt(0)).isTrue();
        assertThat(result.get(2).isNullAt(0)).isTrue();
        assertThat(result.get(2).isNullAt(1)).isTrue();
    }

    @Test
    void testColumnProjection() throws IOException {
        RowType writeType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();
        RowType readType = RowType.builder().field("f_string", DataTypes.STRING()).build();
        Path path = newPath();

        writeRows(
                writeType,
                path,
                GenericRow.of(1, BinaryString.fromString("aaa"), 1.1),
                GenericRow.of(2, BinaryString.fromString("bbb"), 2.2));

        List<InternalRow> result = readAll(writeType, readType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getString(0).toString()).isEqualTo("aaa");
        assertThat(result.get(1).getString(0).toString()).isEqualTo("bbb");
    }

    @Test
    void testLargeDataset() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        int numRows = 10000;
        GenericRow[] rows = new GenericRow[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = GenericRow.of(i, BinaryString.fromString("row" + i));
        }
        writeRows(rowType, path, rows);

        List<InternalRow> result = readAll(rowType, rowType, path, null);
        assertThat(result).hasSize(numRows);
        assertThat(result.get(0).getInt(0)).isEqualTo(0);
        assertThat(result.get(numRows - 1).getInt(0)).isEqualTo(numRows - 1);
    }

    @Test
    void testRowGroupPredicateFiltering() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        Path path = newPath();

        int numRows = 10000;
        GenericRow[] rows = new GenericRow[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = GenericRow.of(i, BinaryString.fromString("v" + i));
        }
        writeRows(rowType, path, "f_int", rows);

        // Predicate that cannot match any row group (all values are 0..9999)
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = builder.greaterThan(0, 99999);
        List<InternalRow> result =
                readAll(rowType, rowType, path, Collections.singletonList(predicate));
        assertThat(result).isEmpty();

        // Predicate that matches the row group (values include range 0..9999)
        Predicate matchPredicate = builder.greaterThan(0, 5000);
        List<InternalRow> matchResult =
                readAll(rowType, rowType, path, Collections.singletonList(matchPredicate));
        assertThat(matchResult).hasSize(numRows);
    }

    @Test
    void testReturnedPosition() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        writeRows(
                rowType,
                path,
                GenericRow.of(1, BinaryString.fromString("a")),
                GenericRow.of(2, BinaryString.fromString("b")),
                GenericRow.of(3, BinaryString.fromString("c")));

        MosaicFileFormat format = createFormat();
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        LocalFileIO fileIO = new LocalFileIO();
        RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
        assertThat(batch).isNotNull();
        FileRecordIterator<InternalRow> fileIter = (FileRecordIterator<InternalRow>) batch;

        fileIter.next();
        assertThat(fileIter.returnedPosition()).isEqualTo(0);
        fileIter.next();
        assertThat(fileIter.returnedPosition()).isEqualTo(1);
        fileIter.next();
        assertThat(fileIter.returnedPosition()).isEqualTo(2);

        reader.close();
    }

    @Test
    void testProjectionWithMissingColumns() throws IOException {
        RowType writeType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        // Read type has a column that doesn't exist in the file (schema evolution)
        RowType readType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_new_col", DataTypes.BIGINT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        Path path = newPath();

        writeRows(
                writeType,
                path,
                GenericRow.of(1, BinaryString.fromString("aaa")),
                GenericRow.of(2, BinaryString.fromString("bbb")));

        List<InternalRow> result = readAll(writeType, readType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).isNullAt(1)).isTrue();
        assertThat(result.get(0).getString(2).toString()).isEqualTo("aaa");
        assertThat(result.get(1).getInt(0)).isEqualTo(2);
        assertThat(result.get(1).isNullAt(1)).isTrue();
        assertThat(result.get(1).getString(2).toString()).isEqualTo("bbb");
    }

    @Test
    void testProjectionAllColumnsMissing() throws IOException {
        RowType writeType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        // Read type has only columns that don't exist in the file
        RowType readType =
                RowType.builder()
                        .field("f_new_a", DataTypes.INT())
                        .field("f_new_b", DataTypes.STRING())
                        .build();
        Path path = newPath();

        writeRows(
                writeType,
                path,
                GenericRow.of(1, BinaryString.fromString("x")),
                GenericRow.of(2, BinaryString.fromString("y")));

        List<InternalRow> result = readAll(writeType, readType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).isNullAt(0)).isTrue();
        assertThat(result.get(0).isNullAt(1)).isTrue();
        assertThat(result.get(1).isNullAt(0)).isTrue();
        assertThat(result.get(1).isNullAt(1)).isTrue();
    }

    @Test
    void testUnsupportedCompressionThrows() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();
        MosaicFileFormat format = createFormat();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        LocalFileIO fileIO = new LocalFileIO();

        assertThatThrownBy(() -> writerFactory.create(fileIO.newOutputStream(path, false), "lz4"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("lz4");
    }

    @Test
    void testReachTargetSize() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();
        MosaicFileFormat format = createFormat();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);

        LocalFileIO fileIO = new LocalFileIO();
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");

        boolean reached = false;
        for (int i = 0; i < 100000; i++) {
            writer.addElement(GenericRow.of(i, BinaryString.fromString("value_" + i + "_padding")));
            if (writer.reachTargetSize(true, 1024)) {
                reached = true;
                break;
            }
        }
        writer.close();
        assertThat(reached).isTrue();
    }

    private Path newPath() {
        return new Path(tempDir.toUri().toString(), UUID.randomUUID() + ".mosaic");
    }

    private void writeRows(RowType rowType, Path path, GenericRow... rows) throws IOException {
        writeRows(rowType, path, "", rows);
    }

    private void writeRows(RowType rowType, Path path, String statsColumns, GenericRow... rows)
            throws IOException {
        MosaicFileFormat format = createFormat(statsColumns);
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        LocalFileIO fileIO = new LocalFileIO();
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
        for (GenericRow row : rows) {
            writer.addElement(row);
        }
        writer.close();
    }

    private List<InternalRow> readAll(
            RowType dataType, RowType readType, Path path, List<Predicate> predicates)
            throws IOException {
        MosaicFileFormat format = createFormat();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataType, readType, predicates);
        LocalFileIO fileIO = new LocalFileIO();
        RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        InternalRowSerializer serializer = new InternalRowSerializer(readType);
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        reader.close();
        return result;
    }

    private static MosaicFileFormat createFormat() {
        return createFormat("");
    }

    private static MosaicFileFormat createFormat(String statsColumns) {
        Options options = new Options();
        if (!statsColumns.isEmpty()) {
            options.set(MosaicFileFormat.STATS_COLUMNS, statsColumns);
        }
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
    }

    private static SimpleStatsProducer disabledStatsProducer() {
        return new SimpleStatsProducer() {

            @Override
            public boolean isStatsDisabled() {
                return true;
            }

            @Override
            public boolean requirePerRecord() {
                return false;
            }

            @Override
            public void collect(InternalRow row) {
                throw new IllegalStateException();
            }

            @Override
            public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
                throw new IllegalStateException();
            }
        };
    }

    private static class CapturingMosaicWriterFactory implements FormatWriterFactory {

        private final FormatWriterFactory delegate;
        private MosaicRecordsWriter writer;

        private CapturingMosaicWriterFactory(FormatWriterFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            FormatWriter created = delegate.create(out, compression);
            writer = (MosaicRecordsWriter) created;
            return created;
        }

        private MosaicRecordsWriter writer() {
            assertThat(writer).isNotNull();
            return writer;
        }
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}

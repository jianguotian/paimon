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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link MosaicRecordsReader}. */
class MosaicRecordsReaderTest {

    @Test
    void testConstructorRuntimeExceptionClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        RuntimeException failure = new RuntimeException("native reader failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testConstructorIOExceptionClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        IOException failure = new IOException("native reader failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isInstanceOf(RuntimeException.class)
                .hasCause(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testConstructorErrorClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        UnsatisfiedLinkError failure = new UnsatisfiedLinkError("native library failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testConstructorFailureAfterReaderCreatedClosesReaderAndOtherResources()
            throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RuntimeException failure = new RuntimeException("schema failed");
        doThrow(failure).when(reader).getSchema();

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> reader))
                .isSameAs(failure);

        verify(reader).close();
        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testCloseContinuesWhenReaderCloseThrows() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        RuntimeException failure = new RuntimeException("reader close failed");
        doThrow(failure).when(reader).close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);

        assertThatThrownBy(recordsReader::close).isSameAs(failure);

        verify(reader).close();
        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testCloseAddsSuppressedExceptionsFromLaterResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        RuntimeException allocatorFailure = new RuntimeException("allocator close failed");
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator(allocatorFailure);
        MosaicReader reader = createReader();
        RuntimeException readerFailure = new RuntimeException("reader close failed");
        doThrow(readerFailure).when(reader).close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);

        assertThatThrownBy(recordsReader::close)
                .isSameAs(readerFailure)
                .satisfies(t -> assertThat(t.getSuppressed()).containsExactly(allocatorFailure));

        verify(reader).close();
        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testAllProjectedColumnsMissingSkipsRowGroupRead() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(3);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);

        assertThat(readerBatchSize(recordsReader)).isEqualTo(3);
        verify(reader, never()).readRowGroup(anyInt(), any());

        recordsReader.close();
    }

    @Test
    void testAllMissingBatchesTrackPositionsIndependently() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        when(reader.numRowGroups()).thenReturn(2);
        when(reader.rowGroupNumRows(0)).thenReturn(2);
        when(reader.rowGroupNumRows(1)).thenReturn(2);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> first = recordsReader.readBatch();
        FileRecordIterator<InternalRow> second = recordsReader.readBatch();

        second.next();
        assertThat(second.returnedPosition()).isEqualTo(2);
        first.next();
        assertThat(first.returnedPosition()).isZero();
        second.next();
        assertThat(second.returnedPosition()).isEqualTo(3);
        first.next();
        assertThat(first.returnedPosition()).isEqualTo(1);

        first.releaseBatch();
        second.releaseBatch();
        verify(reader, never()).readRowGroup(anyInt(), any());
        recordsReader.close();
    }

    @Test
    void testMosaicBatchExposesVectorizedRecordIterator() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        IntVector vector = new IntVector("f0", allocator);
        vector.allocateNew(3);
        vector.setSafe(0, 10);
        vector.setNull(1);
        vector.setSafe(2, 30);
        vector.setValueCount(3);
        VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vector));
        root.setRowCount(3);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(3);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> records = recordsReader.readBatch();

        assertThat(records).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(((VectorizedRecordIterator) records).batch().getNumRows()).isEqualTo(3);
        assertThat(records.next().getInt(0)).isEqualTo(10);
        assertThat(records.returnedPosition()).isZero();
        assertThat(records.next().isNullAt(0)).isTrue();
        assertThat(records.returnedPosition()).isEqualTo(1);
        records.releaseBatch();
        assertThat(allocator.getAllocatedMemory()).isZero();

        recordsReader.close();
    }

    @Test
    void testExactArrowSchemaExposesOriginalArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType(), allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(3);
        vector.setSafe(0, 10);
        vector.setNull(1);
        vector.setSafe(2, 30);
        vector.setValueCount(3);
        root.setRowCount(3);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(3);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> records = recordsReader.readBatch();

        assertThat(records).isInstanceOf(ArrowVectorizedRecordIterator.class);
        ArrowVectorizedRecordIterator arrowRecords = (ArrowVectorizedRecordIterator) records;
        assertThat(arrowRecords.arrowBundle().getVectorSchemaRoot()).isSameAs(root);
        BundleRecords bundle = arrowRecords.arrowBundle();
        assertThat(bundle).isInstanceOf(ArrowBundleRecords.class);
        assertThat(((ArrowBundleRecords) bundle).getVectorSchemaRoot()).isSameAs(root);
        assertThat(arrowRecords.batch().getNumRows()).isEqualTo(3);
        assertThat(records.next().getInt(0)).isEqualTo(10);
        assertThat(records.returnedPosition()).isZero();
        records.releaseBatch();
        records.releaseBatch();
        assertThat(allocator.getAllocatedMemory()).isZero();

        verify(reader, never()).project(any());
        recordsReader.close();
    }

    @Test
    void testReadingNextBatchDoesNotInvalidateOutstandingBatch() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot firstRoot = intRoot(allocator, 10);
        VectorSchemaRoot secondRoot = intRoot(allocator, 20, 30);
        when(reader.getSchema()).thenReturn(firstRoot.getSchema());
        when(reader.numRowGroups()).thenReturn(2);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.rowGroupNumRows(1)).thenReturn(2);
        when(reader.readRowGroup(0, allocator)).thenReturn(firstRoot);
        when(reader.readRowGroup(1, allocator)).thenReturn(secondRoot);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> first = recordsReader.readBatch();
        FileRecordIterator<InternalRow> second = recordsReader.readBatch();

        assertThat(first).isInstanceOf(ArrowVectorizedRecordIterator.class);
        assertThat(second).isInstanceOf(ArrowVectorizedRecordIterator.class);
        assertThat(((VectorizedRecordIterator) first).batch())
                .isNotSameAs(((VectorizedRecordIterator) second).batch());
        assertThat(((ArrowVectorizedRecordIterator) first).arrowBundle().getVectorSchemaRoot())
                .isNotSameAs(
                        ((ArrowVectorizedRecordIterator) second)
                                .arrowBundle()
                                .getVectorSchemaRoot());
        assertThat(((VectorizedRecordIterator) first).batch().getNumRows()).isEqualTo(1);
        assertThat(((VectorizedRecordIterator) second).batch().getNumRows()).isEqualTo(2);
        assertThat(first.next().getInt(0)).isEqualTo(10);
        assertThat(second.next().getInt(0)).isEqualTo(20);
        assertThat(second.next().getInt(0)).isEqualTo(30);
        assertThat(second.next()).isNull();
        assertThat(firstRoot.getVector(0).getObject(0)).isEqualTo(10);

        second.releaseBatch();
        assertThat(firstRoot.getVector(0).getObject(0)).isEqualTo(10);
        assertThat(allocator.getAllocatedMemory()).isGreaterThan(0);
        first.releaseBatch();
        assertThat(allocator.getAllocatedMemory()).isZero();
        recordsReader.close();
    }

    @Test
    void testCloseReleasesOutstandingBatch() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot root = intRoot(allocator, 10);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> batch = recordsReader.readBatch();

        assertThat(allocator.getAllocatedMemory()).isGreaterThan(0);
        recordsReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
        batch.releaseBatch();
    }

    @Test
    void testCloseWaitsForConcurrentBatchRelease() throws Exception {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        BlockingRootAllocator allocator = new BlockingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot root = intRoot(allocator, 10);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        FileRecordIterator<InternalRow> batch = recordsReader.readBatch();
        allocator.blockNextRelease();

        AtomicReference<Throwable> releaseFailure = new AtomicReference<>();
        Thread releaseThread =
                new Thread(
                        () -> {
                            try {
                                batch.releaseBatch();
                            } catch (Throwable t) {
                                releaseFailure.set(t);
                            }
                        });
        releaseThread.start();
        allocator.awaitReleaseStarted();

        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closeThread =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            try {
                                recordsReader.close();
                            } catch (Throwable t) {
                                closeFailure.set(t);
                            }
                        });
        closeThread.start();
        assertThat(closeStarted.await(5, TimeUnit.SECONDS)).isTrue();
        awaitBlocked(closeThread);
        assertThat(allocator.closeCount()).isZero();

        allocator.allowRelease();
        releaseThread.join(TimeUnit.SECONDS.toMillis(5));
        closeThread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(releaseThread.isAlive()).isFalse();
        assertThat(closeThread.isAlive()).isFalse();
        assertThat(releaseFailure.get()).isNull();
        assertThat(closeFailure.get()).isNull();
        assertThat(allocator.getAllocatedMemory()).isZero();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testRowTrackingFallsBackFromOriginalArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(SpecialFields.ROW_ID.name(), DataTypes.BIGINT())
                        .field(SpecialFields.SEQUENCE_NUMBER.name(), DataTypes.BIGINT())
                        .build();
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
        IntVector idVector = (IntVector) root.getVector(0);
        idVector.allocateNew(1);
        idVector.setSafe(0, 7);
        idVector.setValueCount(1);
        BigIntVector rowIdVector = (BigIntVector) root.getVector(1);
        rowIdVector.allocateNew(1);
        rowIdVector.setNull(0);
        rowIdVector.setValueCount(1);
        BigIntVector sequenceVector = (BigIntVector) root.getVector(2);
        sequenceVector.allocateNew(1);
        sequenceVector.setNull(0);
        sequenceVector.setValueCount(1);
        root.setRowCount(1);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType,
                        rowType,
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        Map<String, Integer> systemFields = new LinkedHashMap<>();
        systemFields.put(SpecialFields.ROW_ID.name(), 1);
        systemFields.put(SpecialFields.SEQUENCE_NUMBER.name(), 2);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType,
                        recordsReader,
                        false,
                        false,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        true,
                        1000L,
                        123L,
                        systemFields,
                        null,
                        new Path("file:/tmp/mosaic-reader-test"));
        FileRecordIterator<InternalRow> tracked = dataFileReader.readBatch();

        assertThat(tracked).isNotInstanceOf(VectorizedRecordIterator.class);
        InternalRow row = tracked.next();
        assertThat(row.getInt(0)).isEqualTo(7);
        assertThat(row.getLong(1)).isEqualTo(1000L);
        assertThat(row.getLong(2)).isEqualTo(123L);
        assertThat(root.getVector(1).isNull(0)).isTrue();
        assertThat(root.getVector(2).isNull(0)).isTrue();

        tracked.releaseBatch();
        dataFileReader.close();
    }

    @Test
    void testStoredRowIdIdentityMappingKeepsArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(SpecialFields.ROW_ID.name(), DataTypes.BIGINT())
                        .build();
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
        IntVector idVector = (IntVector) root.getVector(0);
        idVector.allocateNew(1);
        idVector.setSafe(0, 7);
        idVector.setValueCount(1);
        BigIntVector rowIdVector = (BigIntVector) root.getVector(1);
        rowIdVector.allocateNew(1);
        rowIdVector.setSafe(0, 123L);
        rowIdVector.setValueCount(1);
        root.setRowCount(1);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType,
                        rowType,
                        null,
                        filePath,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType,
                        recordsReader,
                        false,
                        false,
                        new int[] {0, 1},
                        null,
                        null,
                        true,
                        null,
                        7L,
                        Collections.singletonMap(SpecialFields.ROW_ID.name(), 1),
                        null,
                        filePath);

        FileRecordIterator<InternalRow> records = dataFileReader.readBatch();

        assertThat(records).isInstanceOf(ArrowVectorizedRecordIterator.class);
        assertThat(((ArrowVectorizedRecordIterator) records).arrowBundle().getVectorSchemaRoot())
                .isSameAs(root);
        InternalRow row = records.next();
        assertThat(row.getInt(0)).isEqualTo(7);
        assertThat(row.getLong(1)).isEqualTo(123L);
        records.releaseBatch();
        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testCompatibleProjectionExposesProjectedArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType dataType =
                RowType.builder()
                        .field("f0", DataTypes.INT())
                        .field("f1", DataTypes.STRING())
                        .build();
        RowType projectedType = RowType.builder().field("f0", DataTypes.INT()).build();
        Schema dataSchema;
        try (VectorSchemaRoot schemaRoot = ArrowUtils.createVectorSchemaRoot(dataType, allocator)) {
            dataSchema = schemaRoot.getSchema();
        }
        VectorSchemaRoot projectedRoot =
                ArrowUtils.createVectorSchemaRoot(projectedType, allocator);
        IntVector vector = (IntVector) projectedRoot.getVector(0);
        vector.allocateNew(1);
        vector.setSafe(0, 42);
        vector.setValueCount(1);
        projectedRoot.setRowCount(1);
        when(reader.getSchema()).thenReturn(dataSchema);
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(projectedRoot);

        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        dataType,
                        projectedType,
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        FileRecordIterator<InternalRow> records = recordsReader.readBatch();

        assertThat(records).isInstanceOf(ArrowVectorizedRecordIterator.class);
        assertThat(((ArrowVectorizedRecordIterator) records).arrowBundle().getVectorSchemaRoot())
                .isSameAs(projectedRoot);
        assertThat(records.next().getInt(0)).isEqualTo(42);
        records.releaseBatch();

        verify(reader).project(new String[] {"f0"});
        recordsReader.close();
    }

    @Test
    void testProjectionWithMissingColumnDoesNotExposeArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType fileType = RowType.builder().field("f0", DataTypes.INT()).build();
        RowType projectedType =
                RowType.builder()
                        .field("f0", DataTypes.INT())
                        .field("missing", DataTypes.STRING())
                        .build();
        Schema fileSchema;
        try (VectorSchemaRoot schemaRoot = ArrowUtils.createVectorSchemaRoot(fileType, allocator)) {
            fileSchema = schemaRoot.getSchema();
        }
        VectorSchemaRoot projectedRoot = ArrowUtils.createVectorSchemaRoot(fileType, allocator);
        IntVector vector = (IntVector) projectedRoot.getVector(0);
        vector.allocateNew(1);
        vector.setSafe(0, 42);
        vector.setValueCount(1);
        projectedRoot.setRowCount(1);
        when(reader.getSchema()).thenReturn(fileSchema);
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(projectedRoot);

        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        projectedType,
                        projectedType,
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        FileRecordIterator<InternalRow> records = recordsReader.readBatch();

        assertThat(records).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(records).isNotInstanceOf(ArrowVectorizedRecordIterator.class);
        InternalRow row = records.next();
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.isNullAt(1)).isTrue();
        records.releaseBatch();

        verify(reader).project(new String[] {"f0"});
        recordsReader.close();
    }

    @Test
    void testAddedTableColumnFallsBackFromArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType fileType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(1, "f1", DataTypes.BIGINT())));
        RowType tableType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(2, "added", DataTypes.STRING()),
                                new DataField(1, "f1", DataTypes.BIGINT())));
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(fileType, allocator);
        IntVector f0 = (IntVector) root.getVector("f0");
        f0.allocateNew(1);
        f0.setSafe(0, 10);
        f0.setValueCount(1);
        BigIntVector f1 = (BigIntVector) root.getVector("f1");
        f1.allocateNew(1);
        f1.setSafe(0, 20L);
        f1.setValueCount(1);
        root.setRowCount(1);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        fileType,
                        fileType,
                        null,
                        filePath,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        tableType,
                        recordsReader,
                        false,
                        false,
                        new int[] {0, -1, 1},
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        null,
                        filePath);

        FileRecordIterator<InternalRow> records = dataFileReader.readBatch();

        assertThat(records).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(records).isNotInstanceOf(ArrowVectorizedRecordIterator.class);
        InternalRow row = records.next();
        assertThat(row.getInt(0)).isEqualTo(10);
        assertThat(row.isNullAt(1)).isTrue();
        assertThat(row.getLong(2)).isEqualTo(20L);
        records.releaseBatch();
        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testRenamedTableColumnKeepsSchemaNeutralArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType fileType =
                new RowType(
                        Collections.singletonList(new DataField(0, "old_name", DataTypes.INT())));
        RowType tableType =
                new RowType(
                        Collections.singletonList(new DataField(0, "new_name", DataTypes.INT())));
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(fileType, allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(1);
        vector.setSafe(0, 42);
        vector.setValueCount(1);
        root.setRowCount(1);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        fileType,
                        fileType,
                        null,
                        filePath,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        tableType,
                        recordsReader,
                        false,
                        false,
                        null,
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        null,
                        filePath);

        FileRecordIterator<InternalRow> records = dataFileReader.readBatch();

        assertThat(records).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(records).isInstanceOf(ArrowVectorizedRecordIterator.class);
        assertThat(((ArrowVectorizedRecordIterator) records).arrowBundle().getRowType())
                .isEqualTo(fileType);
        assertThat(records.next().getInt(0)).isEqualTo(42);
        records.releaseBatch();
        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testIdentityIndexMappingKeepsArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "f0", DataTypes.INT())));
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(1);
        vector.setSafe(0, 42);
        vector.setValueCount(1);
        root.setRowCount(1);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType,
                        rowType,
                        null,
                        filePath,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType,
                        recordsReader,
                        false,
                        false,
                        new int[] {0},
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        null,
                        filePath);

        FileRecordIterator<InternalRow> records = dataFileReader.readBatch();

        assertThat(records).isInstanceOf(ArrowVectorizedRecordIterator.class);
        assertThat(records.next().getInt(0)).isEqualTo(42);
        records.releaseBatch();
        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testReorderedTableColumnsFallBackFromArrowBundle() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType fileType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.INT()),
                                new DataField(1, "b", DataTypes.INT())));
        RowType tableType =
                new RowType(
                        Arrays.asList(
                                new DataField(1, "b", DataTypes.INT()),
                                new DataField(0, "a", DataTypes.INT())));
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(fileType, allocator);
        IntVector a = (IntVector) root.getVector("a");
        a.allocateNew(1);
        a.setSafe(0, 10);
        a.setValueCount(1);
        IntVector b = (IntVector) root.getVector("b");
        b.allocateNew(1);
        b.setSafe(0, 20);
        b.setValueCount(1);
        root.setRowCount(1);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        fileType,
                        fileType,
                        null,
                        filePath,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        tableType,
                        recordsReader,
                        false,
                        false,
                        new int[] {1, 0},
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        null,
                        filePath);

        FileRecordIterator<InternalRow> records = dataFileReader.readBatch();

        assertThat(records).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(records).isNotInstanceOf(ArrowVectorizedRecordIterator.class);
        InternalRow row = records.next();
        assertThat(row.getInt(0)).isEqualTo(20);
        assertThat(row.getInt(1)).isEqualTo(10);
        records.releaseBatch();
        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    private static MosaicInputFileAdapter createInputFileAdapter(
            CloseCountingSeekableInputStream inputStream) throws IOException {
        return new MosaicInputFileAdapter(
                new CloseCountingFileIO(inputStream), new Path("file:/tmp/mosaic-reader-test"));
    }

    private static MosaicRecordsReader createRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            CloseCountingRootAllocator allocator,
            MosaicReader reader) {
        return new MosaicRecordsReader(
                inputFileAdapter,
                0,
                rowType(),
                rowType(),
                null,
                new Path("file:/tmp/mosaic-reader-test"),
                allocator,
                (inputFile, fileSize, bufferAllocator) -> reader);
    }

    private static MosaicReader createReader() {
        MosaicReader reader = mock(MosaicReader.class);
        when(reader.getSchema()).thenReturn(new Schema(Collections.emptyList()));
        return reader;
    }

    private static int readerBatchSize(MosaicRecordsReader recordsReader) throws IOException {
        int count = 0;
        while (true) {
            FileRecordIterator<InternalRow> batch = recordsReader.readBatch();
            if (batch == null) {
                return count;
            }
            InternalRow row;
            while ((row = batch.next()) != null) {
                assertThat(row.isNullAt(0)).isTrue();
                count++;
            }
            batch.releaseBatch();
        }
    }

    private static RowType rowType() {
        return DataTypes.ROW(DataTypes.INT());
    }

    private static VectorSchemaRoot intRoot(RootAllocator allocator, int... values) {
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType(), allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i]);
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    private static class CloseCountingFileIO extends LocalFileIO {

        private final CloseCountingSeekableInputStream inputStream;

        private CloseCountingFileIO(CloseCountingSeekableInputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) {
            return inputStream;
        }
    }

    private static class CloseCountingSeekableInputStream extends SeekableInputStream {

        private int closeCount;

        @Override
        public void seek(long desired) {}

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return -1;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public void close() {
            closeCount++;
        }

        int closeCount() {
            return closeCount;
        }
    }

    private static class CloseCountingRootAllocator extends RootAllocator {

        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseCountingRootAllocator() {
            this(null);
        }

        private CloseCountingRootAllocator(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
            super.close();
        }

        int closeCount() {
            return closeCount;
        }
    }

    private static class BlockingRootAllocator extends CloseCountingRootAllocator {

        private final CountDownLatch releaseStarted = new CountDownLatch(1);
        private final CountDownLatch allowRelease = new CountDownLatch(1);
        private volatile boolean blockNextRelease;

        private void blockNextRelease() {
            blockNextRelease = true;
        }

        private void awaitReleaseStarted() throws InterruptedException {
            assertThat(releaseStarted.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void allowRelease() {
            allowRelease.countDown();
        }

        @Override
        public void releaseBytes(long size) {
            if (blockNextRelease) {
                blockNextRelease = false;
                releaseStarted.countDown();
                try {
                    assertThat(allowRelease.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            super.releaseBytes(size);
        }
    }

    private static void awaitBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(thread.getState()).isEqualTo(Thread.State.BLOCKED);
    }

    @Test
    void testAllProjectedColumnsMissingPreservesSelectedPositionsAcrossRowGroups()
            throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        when(reader.numRowGroups()).thenReturn(3);
        when(reader.rowGroupNumRows(0)).thenReturn(2);
        when(reader.rowGroupNumRows(1)).thenReturn(2);
        when(reader.rowGroupNumRows(2)).thenReturn(2);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(1, 5);
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        filePath,
                        selection,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader,
                        3,
                        MosaicFileFormat.READ_PREFETCH_MAX_BYTES.defaultValue().getBytes());
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType(),
                        recordsReader,
                        false,
                        false,
                        null,
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        selection,
                        filePath);

        FileRecordIterator<InternalRow> firstBatch = dataFileReader.readBatch();
        assertThat(firstBatch).isNotNull();
        InternalRow firstRow = firstBatch.next();
        assertThat(firstRow).isNotNull();
        assertThat(firstRow.isNullAt(0)).isTrue();
        assertThat(firstBatch.returnedPosition()).isEqualTo(1);
        assertThat(firstBatch.next()).isNull();
        firstBatch.releaseBatch();

        FileRecordIterator<InternalRow> secondBatch = dataFileReader.readBatch();
        assertThat(secondBatch).isNotNull();
        InternalRow secondRow = secondBatch.next();
        assertThat(secondRow).isNotNull();
        assertThat(secondRow.isNullAt(0)).isTrue();
        assertThat(secondBatch.returnedPosition()).isEqualTo(5);
        assertThat(secondBatch.next()).isNull();
        secondBatch.releaseBatch();

        assertThat(dataFileReader.readBatch()).isNull();
        verify(reader, never()).readRowGroup(anyInt(), any());

        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testPrefetchSkipsUnselectedRowGroupsAndPreservesPositions() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot firstRoot = intRoot(allocator, 0, 1);
        VectorSchemaRoot thirdRoot = intRoot(allocator, 5, 6);
        VectorSchemaRoot fourthRoot = intRoot(allocator, 7, 8, 9);
        when(reader.getSchema()).thenReturn(firstRoot.getSchema());
        when(reader.numRowGroups()).thenReturn(4);
        when(reader.rowGroupNumRows(0)).thenReturn(2);
        when(reader.rowGroupNumRows(1)).thenReturn(3);
        when(reader.rowGroupNumRows(2)).thenReturn(2);
        when(reader.rowGroupNumRows(3)).thenReturn(3);
        when(reader.readRowGroup(0, allocator)).thenReturn(firstRoot);
        when(reader.readRowGroup(2, allocator)).thenReturn(thirdRoot);
        when(reader.readRowGroup(3, allocator)).thenReturn(fourthRoot);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(1, 5, 9);
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        filePath,
                        selection,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader,
                        4,
                        MosaicFileFormat.READ_PREFETCH_MAX_BYTES.defaultValue().getBytes());
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType(),
                        recordsReader,
                        false,
                        false,
                        null,
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        selection,
                        filePath);

        assertSelectedRow(dataFileReader.readBatch(), 1, 1);
        assertSelectedRow(dataFileReader.readBatch(), 5, 5);
        assertSelectedRow(dataFileReader.readBatch(), 9, 9);
        assertThat(dataFileReader.readBatch()).isNull();

        verify(reader).readRowGroup(0, allocator);
        verify(reader, never()).readRowGroup(1, allocator);
        verify(reader).readRowGroup(2, allocator);
        verify(reader).readRowGroup(3, allocator);

        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testPrefetchPreservesPositionAfterPartiallyConsumedBatch() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot secondRoot = intRoot(allocator, 3, 4, 5, 6);
        VectorSchemaRoot thirdRoot = intRoot(allocator, 7, 8);
        when(reader.getSchema()).thenReturn(secondRoot.getSchema());
        when(reader.numRowGroups()).thenReturn(3);
        when(reader.rowGroupNumRows(0)).thenReturn(3);
        when(reader.rowGroupNumRows(1)).thenReturn(4);
        when(reader.rowGroupNumRows(2)).thenReturn(2);
        when(reader.readRowGroup(1, allocator)).thenReturn(secondRoot);
        when(reader.readRowGroup(2, allocator)).thenReturn(thirdRoot);

        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(3, 7);
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        selection,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader,
                        2,
                        MosaicFileFormat.READ_PREFETCH_MAX_BYTES.defaultValue().getBytes());

        FileRecordIterator<InternalRow> secondBatch =
                recordsReader.readBatch().selection(RoaringBitmap32.bitmapOf(3));
        assertThat(secondBatch.next().getInt(0)).isEqualTo(3);
        assertThat(secondBatch.returnedPosition()).isEqualTo(3);
        assertThat(secondBatch.next()).isNull();
        secondBatch.releaseBatch();

        FileRecordIterator<InternalRow> thirdBatch =
                recordsReader.readBatch().selection(RoaringBitmap32.bitmapOf(7));
        assertThat(thirdBatch.next().getInt(0)).isEqualTo(7);
        assertThat(thirdBatch.returnedPosition()).isEqualTo(7);
        assertThat(thirdBatch.next()).isNull();
        thirdBatch.releaseBatch();
        assertThat(recordsReader.readBatch()).isNull();

        verify(reader, never()).readRowGroup(0, allocator);
        verify(reader).readRowGroup(1, allocator);
        verify(reader).readRowGroup(2, allocator);

        recordsReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testDisabledPrefetchReadsRowGroupsOnDemand() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 3);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 0);

        FileRecordIterator<InternalRow> first = recordsReader.readBatch();
        assertThat(first).isNotNull();
        // Depth 0 must not read the next row group before the first batch is consumed.
        verify(reader, times(1)).readRowGroup(anyInt(), any());
        assertThat(first.next().getInt(0)).isEqualTo(0);
        assertThat(first.next()).isNull();
        first.releaseBatch();

        List<Integer> values = new ArrayList<>();
        FileRecordIterator<InternalRow> batch;
        while ((batch = recordsReader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
                values.add(row.getInt(0));
            }
            batch.releaseBatch();
        }
        assertThat(values).containsExactly(1, 2);
        verify(reader, times(3)).readRowGroup(anyInt(), any());

        recordsReader.close();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testInterruptedReadKeepsInFlightRowGroupUntilClose() throws Exception {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 1);
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        doAnswer(
                        invocation -> {
                            readStarted.countDown();
                            releaseRead.await();
                            events.add("read-finished");
                            return rowGroup(allocator, 0);
                        })
                .when(reader)
                .readRowGroup(eq(0), any());
        doAnswer(
                        invocation -> {
                            events.add("reader-closed");
                            return null;
                        })
                .when(reader)
                .close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 2);
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        Thread consumer =
                new Thread(
                        () -> {
                            try {
                                recordsReader.readBatch();
                            } catch (Throwable t) {
                                readFailure.set(t);
                            }
                        });
        consumer.start();
        readStarted.await();
        consumer.interrupt();
        consumer.join();
        assertThat(readFailure.get()).isInstanceOf(InterruptedIOException.class);

        // The interrupted read is still running natively: close() has to wait for it.
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = new Thread(() -> closeQuietly(recordsReader, closeFailure));
        closer.start();
        closer.join(200);
        assertThat(closer.isAlive()).isTrue();
        assertThat(events).isEmpty();
        releaseRead.countDown();
        closer.join();

        assertThat(closeFailure.get()).isNull();
        assertThat(events).containsExactly("read-finished", "reader-closed");
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testCloseWithInterruptFlagStillWaitsForInFlightRowGroups() throws Exception {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 2);
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        doAnswer(
                        invocation -> {
                            readStarted.countDown();
                            releaseRead.await();
                            events.add("read-finished");
                            return rowGroup(allocator, 1);
                        })
                .when(reader)
                .readRowGroup(eq(1), any());
        doAnswer(
                        invocation -> {
                            events.add("reader-closed");
                            return null;
                        })
                .when(reader)
                .close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 1);
        // Consuming row group 0 schedules row group 1, which now blocks in the background.
        assertThat(recordsReader.readBatch()).isNotNull();
        readStarted.await();

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        AtomicBoolean interruptedAfterClose = new AtomicBoolean();
        Thread closer =
                new Thread(
                        () -> {
                            Thread.currentThread().interrupt();
                            closeQuietly(recordsReader, closeFailure);
                            interruptedAfterClose.set(Thread.currentThread().isInterrupted());
                        });
        closer.start();
        closer.join(200);
        assertThat(closer.isAlive()).isTrue();
        assertThat(events).isEmpty();
        releaseRead.countDown();
        closer.join();

        assertThat(closeFailure.get()).isNull();
        assertThat(events).containsExactly("read-finished", "reader-closed");
        assertThat(interruptedAfterClose).isTrue();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testRefillFailureLeavesCurrentRowGroupReleasable() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 3);
        RuntimeException failure = new RuntimeException("row group 2 metadata failed");
        when(reader.rowGroupNumRows(2)).thenThrow(failure);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 1);
        // Row group 0 is handed over; scheduling row group 2 fails while refilling behind it.
        assertThat(recordsReader.readBatch()).isNotNull();
        assertThatThrownBy(recordsReader::readBatch).isSameAs(failure);

        // Row group 0 must still be released, otherwise the allocator reports a leak here.
        recordsReader.close();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @CsvSource({
        "0, 1, 1",
        "4000, 1, 1",
        "4000, 4, 4",
        "5000, 1, 2",
        "9999, 1, 2",
        "10000, 1, 3",
        "100000, 1, 4"
    })
    void testPrefetchIsBoundedByEstimatedDecodedBytes(
            long budget, int batchesToRead, int expectedReads) throws IOException {
        // One INT column: 5 bytes per row; 1,000 rows per row group is 5,000 bytes.
        assertThat(MosaicRecordsReader.estimatedRowBytes(rowType())).isEqualTo(5);
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 4);
        when(reader.rowGroupNumRows(anyInt())).thenReturn(1000);
        try (MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader,
                        8,
                        budget)) {
            for (int i = 0; i < batchesToRead; i++) {
                FileRecordIterator<InternalRow> batch = recordsReader.readBatch();
                assertThat(batch).isNotNull();
                assertThat(batch.next().getInt(0)).isEqualTo(i);
                batch.releaseBatch();
            }
        }
        // Closing drains scheduled reads, so verification cannot race with background tasks.
        verify(reader, times(expectedReads)).readRowGroup(anyInt(), any());
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    private static void closeQuietly(
            MosaicRecordsReader recordsReader, AtomicReference<Throwable> failure) {
        try {
            recordsReader.close();
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    /** A mocked native reader whose file schema contains the projected column. */
    private static MosaicReader createProjectedReader(BufferAllocator allocator, int numRowGroups)
            throws IOException {
        MosaicReader reader = mock(MosaicReader.class);
        when(reader.getSchema())
                .thenReturn(
                        new Schema(
                                Collections.singletonList(
                                        Field.nullable("f0", new ArrowType.Int(32, true)))));
        when(reader.numRowGroups()).thenReturn(numRowGroups);
        when(reader.rowGroupNumRows(anyInt())).thenReturn(1);
        when(reader.readRowGroup(anyInt(), any()))
                .thenAnswer(invocation -> rowGroup(allocator, invocation.getArgument(0)));
        return reader;
    }

    private static VectorSchemaRoot rowGroup(BufferAllocator allocator, int value) {
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType(), allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(1);
        vector.set(0, value);
        vector.setValueCount(1);
        root.setRowCount(1);
        return root;
    }

    private static void assertSelectedRow(
            FileRecordIterator<InternalRow> batch, int value, long position) throws IOException {
        assertThat(batch).isNotNull();
        assertThat(batch.next().getInt(0)).isEqualTo(value);
        assertThat(batch.returnedPosition()).isEqualTo(position);
        assertThat(batch.next()).isNull();
        batch.releaseBatch();
    }

    private static MosaicRecordsReader createRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            CloseCountingRootAllocator allocator,
            MosaicReader reader,
            int prefetchRowGroups) {
        return new MosaicRecordsReader(
                inputFileAdapter,
                0,
                rowType(),
                rowType(),
                null,
                new Path("file:/tmp/mosaic-reader-test"),
                allocator,
                (inputFile, fileSize, bufferAllocator) -> reader,
                prefetchRowGroups,
                MosaicFileFormat.READ_PREFETCH_MAX_BYTES.defaultValue().getBytes());
    }
}

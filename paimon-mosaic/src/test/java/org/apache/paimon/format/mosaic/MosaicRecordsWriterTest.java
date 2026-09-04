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
import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.io.VectorizedBundleRecords;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Test for {@link MosaicRecordsWriter}. */
class MosaicRecordsWriterTest {

    private static final FileFormatFactory.FormatContext FORMAT_CONTEXT =
            new FileFormatFactory.FormatContext(new Options(), 1024, 1024);

    @Test
    void testConstructorFailureClosesCreatedResources() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        RuntimeException failure = new RuntimeException("native writer failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsWriter(
                                        new ByteArrayOutputStream(),
                                        rowType,
                                        FORMAT_CONTEXT,
                                        Collections.emptyList(),
                                        null,
                                        allocator,
                                        (outputStream, arrowSchema, options, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testCompatibleArrowBundleUsesDirectWrite() throws Exception {
        RowType rowType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator writerAllocator = new RootAllocator();
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, writerAllocator);

        try (BufferAllocator sourceAllocator =
                        writerAllocator.newChildAllocator("same-root-direct", 0, Long.MAX_VALUE);
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            IntVector a = (IntVector) root.getVector("a");
            IntVector b = (IntVector) root.getVector("b");
            setInt(a, 10);
            setInt(b, 20);
            root.setRowCount(1);

            writer.writeBundle(new ArrowBundleRecords(root, rowType, true));

            verify(nativeWriter).write(same(root));
            assertThat(writer.directArrowRows()).isEqualTo(1);
            assertThat(writer.schemaCompatibilityFallbackRows()).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void testVectorizedBundleUsesColumnBatchWrite() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.INT()).build();
        List<List<Integer>> snapshots = new ArrayList<>();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot root = invocation.getArgument(0);
                            List<Integer> values = new ArrayList<>();
                            IntVector vector = (IntVector) root.getVector("a");
                            for (int i = 0; i < root.getRowCount(); i++) {
                                values.add(vector.get(i));
                            }
                            snapshots.add(values);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter);

        try {
            writer.addElement(GenericRow.of(1));
            HeapIntVector vector = new HeapIntVector(2);
            vector.setInt(0, 2);
            vector.setInt(1, 3);
            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(2);
            writer.writeBundle(new VectorizedBundleRecords(batch, null));
        } finally {
            writer.close();
        }

        assertThat(snapshots).containsExactly(Collections.singletonList(1), Arrays.asList(2, 3));
        assertThat(writer.genericBundleRows()).isZero();
    }

    @Test
    void testVectorizedBundlePreservesSelectionOrderAndDuplicatesAcrossBatches() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.INT()).build();
        List<List<Integer>> snapshots = new ArrayList<>();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot root = invocation.getArgument(0);
                            List<Integer> values = new ArrayList<>();
                            IntVector vector = (IntVector) root.getVector("a");
                            for (int i = 0; i < root.getRowCount(); i++) {
                                values.add(vector.get(i));
                            }
                            snapshots.add(values);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter);

        try {
            int sourceRows = 2050;
            HeapIntVector vector = new HeapIntVector(sourceRows);
            int[] selected = new int[1025];
            for (int i = 0; i < sourceRows; i++) {
                vector.setInt(i, i);
            }
            for (int i = 0; i < selected.length - 1; i++) {
                selected[i] = sourceRows - 1 - i;
            }
            selected[selected.length - 1] = selected[0];
            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(sourceRows);
            writer.writeBundle(new VectorizedBundleRecords(batch, selected));
        } finally {
            writer.close();
        }

        assertThat(snapshots).hasSize(2);
        assertThat(snapshots.get(0)).hasSize(1024);
        assertThat(snapshots.get(0).get(0)).isEqualTo(2049);
        assertThat(snapshots.get(0).get(1023)).isEqualTo(1026);
        assertThat(snapshots.get(1)).containsExactly(2049);
        assertThat(writer.genericBundleRows()).isZero();
    }

    @Test
    void testVectorizedArrayPreservesSelectionOrderAndDuplicatesAcrossBatches() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.ARRAY(DataTypes.INT())).build();
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 2);
        List<List<List<Integer>>> snapshots = new ArrayList<>();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot root = invocation.getArgument(0);
                            List<List<Integer>> values = new ArrayList<>();
                            for (InternalRow row :
                                    new ArrowBatchReader(rowType, true).readBatch(root)) {
                                values.add(row.isNullAt(0) ? null : toInts(row.getArray(0)));
                            }
                            snapshots.add(values);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext);

        try {
            HeapIntVector elements = heapInts(-1, -2, -3, -4, 30, 31, 32, -5, 40);
            HeapArrayVector vector = new HeapArrayVector(4, elements);
            vector.setNullAt(0);
            vector.putOffsetLength(1, 1, 0);
            vector.putOffsetLength(2, 4, 3);
            vector.putOffsetLength(3, 8, 1);
            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(4);
            writer.writeBundle(new VectorizedBundleRecords(batch, new int[] {2, 0, 1, 3, 2}));
        } finally {
            writer.close();
        }

        assertThat(snapshots)
                .containsExactly(
                        Arrays.asList(Arrays.asList(30, 31, 32), null),
                        Arrays.asList(Collections.emptyList(), Collections.singletonList(40)),
                        Collections.singletonList(Arrays.asList(30, 31, 32)));
    }

    @Test
    void testVectorizedMapPreservesSelectionOrderAndDuplicatesAcrossBatches() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("m", DataTypes.MAP(DataTypes.INT(), DataTypes.INT()))
                        .build();
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 2);
        List<List<List<Integer>>> keySnapshots = new ArrayList<>();
        List<List<List<Integer>>> valueSnapshots = new ArrayList<>();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot root = invocation.getArgument(0);
                            List<List<Integer>> keys = new ArrayList<>();
                            List<List<Integer>> values = new ArrayList<>();
                            for (InternalRow row :
                                    new ArrowBatchReader(rowType, true).readBatch(root)) {
                                if (row.isNullAt(0)) {
                                    keys.add(null);
                                    values.add(null);
                                } else {
                                    InternalMap map = row.getMap(0);
                                    keys.add(toInts(map.keyArray()));
                                    values.add(toInts(map.valueArray()));
                                }
                            }
                            keySnapshots.add(keys);
                            valueSnapshots.add(values);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext);

        try {
            HeapMapVector vector =
                    new HeapMapVector(
                            4,
                            heapInts(-1, -2, -3, -4, 4, 5, 6, -5, 8),
                            heapInts(-10, -20, -30, -40, 40, 50, 60, -50, 80));
            vector.setNullAt(0);
            vector.putOffsetLength(1, 1, 0);
            vector.putOffsetLength(2, 4, 3);
            vector.putOffsetLength(3, 8, 1);
            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(4);
            writer.writeBundle(new VectorizedBundleRecords(batch, new int[] {2, 0, 1, 3, 2}));
        } finally {
            writer.close();
        }

        assertThat(keySnapshots)
                .containsExactly(
                        Arrays.asList(Arrays.asList(4, 5, 6), null),
                        Arrays.asList(Collections.emptyList(), Collections.singletonList(8)),
                        Collections.singletonList(Arrays.asList(4, 5, 6)));
        assertThat(valueSnapshots)
                .containsExactly(
                        Arrays.asList(Arrays.asList(40, 50, 60), null),
                        Arrays.asList(Collections.emptyList(), Collections.singletonList(80)),
                        Collections.singletonList(Arrays.asList(40, 50, 60)));
    }

    @Test
    void testVectorizedBundleRespectsWriteBatchMemory() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.STRING()).build();
        MemorySize memoryLimit = MemorySize.parse("64 kb");
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 1024, memoryLimit);
        List<Integer> batchRowCounts = new ArrayList<>();
        List<Long> batchBufferSizes = new ArrayList<>();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot root = invocation.getArgument(0);
                            batchRowCounts.add(root.getRowCount());
                            batchBufferSizes.add(
                                    root.getFieldVectors().stream()
                                            .mapToLong(FieldVector::getBufferSize)
                                            .sum());
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext);

        try {
            int sourceRows = 128;
            byte[] value = new byte[1024];
            HeapBytesVector vector = new HeapBytesVector(sourceRows);
            for (int i = 0; i < sourceRows; i++) {
                vector.putByteArray(i, value, 0, value.length);
            }
            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(sourceRows);
            writer.writeBundle(new VectorizedBundleRecords(batch, null));
        } finally {
            writer.close();
        }

        assertThat(batchRowCounts).hasSizeGreaterThan(1);
        assertThat(batchRowCounts).anyMatch(rows -> rows > 32);
        assertThat(batchRowCounts.stream().mapToInt(Integer::intValue).sum()).isEqualTo(128);
        assertThat(batchBufferSizes).allMatch(size -> size <= memoryLimit.getBytes());
        assertThat(writer.genericBundleRows()).isZero();
    }

    @Test
    void testVectorizedBundleRetriesWhenSampleUnderestimatesMemory() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.STRING()).build();
        MemorySize memoryLimit = MemorySize.parse("64 kb");
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 1024, memoryLimit);
        List<Integer> writtenLengths = new ArrayList<>();
        List<Long> allocatedMemory = new ArrayList<>();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot root = invocation.getArgument(0);
                            VarCharVector vector = (VarCharVector) root.getVector("a");
                            for (int i = 0; i < root.getRowCount(); i++) {
                                writtenLengths.add(vector.get(i).length);
                            }
                            allocatedMemory.add(vector.getAllocator().getAllocatedMemory());
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext);

        try {
            int sourceRows = 64;
            HeapBytesVector vector = new HeapBytesVector(sourceRows);
            for (int i = 0; i < sourceRows; i++) {
                int length = i < 32 ? 1 : 4096;
                vector.putByteArray(i, new byte[length], 0, length);
            }
            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(sourceRows);
            writer.writeBundle(new VectorizedBundleRecords(batch, null));
        } finally {
            writer.close();
        }

        assertThat(writtenLengths)
                .containsExactlyElementsOf(
                        IntStream.range(0, 64)
                                .map(i -> i < 32 ? 1 : 4096)
                                .boxed()
                                .collect(Collectors.toList()));
        assertThat(allocatedMemory).allMatch(size -> size <= 2 * memoryLimit.getBytes());
        assertThat(writer.genericBundleRows()).isZero();
    }

    @Test
    void testNativeWriteFailureIsNotRetriedDuringClose() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.INT()).build();
        RuntimeException failure = new RuntimeException("native write failed");
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doThrow(failure).when(nativeWriter).write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter);

        HeapIntVector vector = new HeapIntVector(1);
        vector.setInt(0, 1);
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
        batch.setNumRows(1);

        assertThatThrownBy(() -> writer.writeBundle(new VectorizedBundleRecords(batch, null)))
                .isSameAs(failure);
        writer.close();

        verify(nativeWriter, times(1)).write(any(VectorSchemaRoot.class));
    }

    @Test
    void testCompatibleCrossRootGenericArrowBundleUsesDirectWrite() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.INT()).build();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator writerAllocator = new RootAllocator();
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, writerAllocator);

        try (RootAllocator sourceAllocator = new RootAllocator();
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            assertThat(sourceAllocator.getRoot()).isNotSameAs(writerAllocator.getRoot());
            setInt((IntVector) root.getVector("a"), 10);
            root.setRowCount(1);

            writer.writeBundle(new ArrowBundleRecords(root, rowType, true));

            verify(nativeWriter).write(same(root));
            assertThat(writer.directArrowRows()).isEqualTo(1);
            assertThat(writer.schemaCompatibilityFallbackRows()).isZero();
            assertThat(writer.genericBundleRows()).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void testCompatibleCrossRootMosaicBundleUsesDirectWrite() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.INT()).build();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator writerAllocator = new RootAllocator();
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, writerAllocator);

        try (RootAllocator sourceAllocator = new RootAllocator();
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            assertThat(sourceAllocator.getRoot()).isNotSameAs(writerAllocator.getRoot());
            setInt((IntVector) root.getVector("a"), 10);
            root.setRowCount(1);

            writer.writeBundle(new MosaicArrowBundleRecords(root, rowType));

            verify(nativeWriter).write(same(root));
            assertThat(writer.directArrowRows()).isEqualTo(1);
            assertThat(writer.schemaCompatibilityFallbackRows()).isZero();
            assertThat(writer.genericBundleRows()).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void testCompatibleMixedRootArrowBundleUsesDirectWrite() throws Exception {
        RowType rowType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator writerAllocator = new RootAllocator();
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, writerAllocator);

        try (RootAllocator firstAllocator = new RootAllocator();
                RootAllocator secondAllocator = new RootAllocator()) {
            IntVector a =
                    (IntVector)
                            ArrowUtils.createVector(
                                    rowType.getFields().get(0), firstAllocator, true);
            IntVector b =
                    (IntVector)
                            ArrowUtils.createVector(
                                    rowType.getFields().get(1), secondAllocator, true);
            try (VectorSchemaRoot root = VectorSchemaRoot.of(a, b)) {
                setInt(a, 10);
                setInt(b, 20);
                root.setRowCount(1);

                writer.writeBundle(new ArrowBundleRecords(root, rowType, true));

                verify(nativeWriter).write(same(root));
                assertThat(writer.directArrowRows()).isEqualTo(1);
                assertThat(writer.schemaCompatibilityFallbackRows()).isZero();
                assertThat(writer.genericBundleRows()).isZero();
            }
        } finally {
            writer.close();
        }
    }

    @Test
    void testCompatibleArrowBundleIgnoresTopLevelRowNullability() throws Exception {
        RowType writerType = RowType.builder().field("a", DataTypes.INT()).build().notNull();
        RowType bundleType = writerType.copy(true);
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator writerAllocator = new RootAllocator();
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter, writerAllocator);

        try (BufferAllocator sourceAllocator =
                        writerAllocator.newChildAllocator(
                                "same-root-nullability", 0, Long.MAX_VALUE);
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(bundleType, sourceAllocator)) {
            setInt((IntVector) root.getVector("a"), 10);
            root.setRowCount(1);

            writer.writeBundle(new MosaicArrowBundleRecords(root, bundleType));

            verify(nativeWriter).write(same(root));
        } finally {
            writer.close();
        }
    }

    @Test
    void testDirectSchemaCacheDoesNotTrustRowTypeIdentity() throws Exception {
        RowType writerType =
                new RowType(Collections.singletonList(new DataField(7, "a", DataTypes.INT())));
        RowType conflictingSchemaType =
                new RowType(Collections.singletonList(new DataField(8, "a", DataTypes.INT())));
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator writerAllocator = new RootAllocator();
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter, writerAllocator);

        try (BufferAllocator sourceAllocator =
                        writerAllocator.newChildAllocator(
                                "same-root-schema-cache", 0, Long.MAX_VALUE);
                VectorSchemaRoot firstRoot =
                        ArrowUtils.createVectorSchemaRoot(writerType, sourceAllocator);
                VectorSchemaRoot secondRoot =
                        ArrowUtils.createVectorSchemaRoot(conflictingSchemaType, sourceAllocator)) {
            setInt((IntVector) firstRoot.getVector("a"), 10);
            firstRoot.setRowCount(1);
            setInt((IntVector) secondRoot.getVector("a"), 20);
            secondRoot.setRowCount(1);

            writer.writeBundle(new MosaicArrowBundleRecords(firstRoot, writerType));
            writer.writeBundle(new MosaicArrowBundleRecords(secondRoot, writerType));

            verify(nativeWriter).write(same(firstRoot));
            verify(nativeWriter, never()).write(same(secondRoot));
            assertThat(writer.schemaCompatibilityFallbackRows()).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void testRenamedMosaicBundleFallsBackToRows() throws Exception {
        RowType writerType =
                new RowType(
                        Collections.singletonList(new DataField(7, "new_name", DataTypes.INT())));
        RowType bundleType =
                new RowType(
                        Collections.singletonList(new DataField(7, "old_name", DataTypes.INT())));
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot written = invocation.getArgument(0);
                            assertThat(written.getVector("new_name").getObject(0)).isEqualTo(10);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter);
        VectorSchemaRoot root;

        try (RootAllocator sourceAllocator = new RootAllocator()) {
            root = ArrowUtils.createVectorSchemaRoot(bundleType, sourceAllocator);
            try (VectorSchemaRoot ignored = root) {
                setInt((IntVector) root.getVector("old_name"), 10);
                root.setRowCount(1);

                writer.writeBundle(new MosaicArrowBundleRecords(root, bundleType));

                verify(nativeWriter, never()).write(same(root));
                assertThat(writer.directArrowRows()).isZero();
                assertThat(writer.schemaCompatibilityFallbackRows()).isEqualTo(1);
            }
        } finally {
            writer.close();
        }

        verify(nativeWriter).write(any(VectorSchemaRoot.class));
        verify(nativeWriter, never()).write(same(root));
    }

    @Test
    void testGenericRenamedArrowBundleFallsBackToRows() throws Exception {
        RowType writerType =
                new RowType(
                        Collections.singletonList(new DataField(7, "new_name", DataTypes.INT())));
        RowType bundleType =
                new RowType(
                        Collections.singletonList(new DataField(7, "old_name", DataTypes.INT())));
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot written = invocation.getArgument(0);
                            assertThat(written.getVector("new_name").getObject(0)).isEqualTo(10);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter);
        VectorSchemaRoot root;

        try (RootAllocator sourceAllocator = new RootAllocator()) {
            root = ArrowUtils.createVectorSchemaRoot(bundleType, sourceAllocator);
            try (VectorSchemaRoot ignored = root) {
                setInt((IntVector) root.getVector("old_name"), 10);
                root.setRowCount(1);

                writer.writeBundle(new ArrowBundleRecords(root, bundleType, true));

                verify(nativeWriter, never()).write(same(root));
                assertThat(writer.directArrowRows()).isZero();
                assertThat(writer.schemaCompatibilityFallbackRows()).isZero();
                assertThat(writer.genericBundleRows()).isEqualTo(1);
            }
        } finally {
            writer.close();
        }

        verify(nativeWriter).write(any(VectorSchemaRoot.class));
        verify(nativeWriter, never()).write(same(root));
    }

    @Test
    void testMosaicBundleRowTypeMismatchFallsBackToRows() throws Exception {
        RowType writerType =
                new RowType(
                        Collections.singletonList(new DataField(7, "new_name", DataTypes.INT())));
        RowType bundleType =
                new RowType(
                        Collections.singletonList(new DataField(7, "old_name", DataTypes.INT())));
        RowType rootType =
                new RowType(
                        Collections.singletonList(new DataField(7, "other_name", DataTypes.INT())));
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter);

        try (RootAllocator sourceAllocator = new RootAllocator();
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rootType, sourceAllocator)) {
            setInt((IntVector) root.getVector("other_name"), 10);
            root.setRowCount(1);

            writer.writeBundle(new MosaicArrowBundleRecords(root, bundleType));
            verify(nativeWriter, never()).write(same(root));
            assertThat(writer.schemaCompatibilityFallbackRows()).isEqualTo(1);
        } finally {
            writer.close();
        }
        verify(nativeWriter).write(any(VectorSchemaRoot.class));
    }

    @Test
    void testReorderedArrowBundleFallsBackToRows() throws Exception {
        RowType writerType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot written = invocation.getArgument(0);
                            assertThat(((IntVector) written.getVector("a")).get(0)).isEqualTo(10);
                            assertThat(((IntVector) written.getVector("b")).get(0)).isEqualTo(20);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter);
        VectorSchemaRoot root;

        try (RootAllocator sourceAllocator = new RootAllocator();
                IntVector b = new IntVector("b", sourceAllocator);
                IntVector a = new IntVector("a", sourceAllocator)) {
            root = new VectorSchemaRoot(Arrays.asList(b, a));
            try (VectorSchemaRoot ignored = root) {
                setInt(b, 20);
                setInt(a, 10);
                root.setRowCount(1);

                writer.writeBundle(new ArrowBundleRecords(root, writerType, true));

                verify(nativeWriter, never()).write(same(root));
                assertThat(writer.genericBundleRows()).isEqualTo(1);
            }
        } finally {
            writer.close();
        }

        verify(nativeWriter).write(any(VectorSchemaRoot.class));
        verify(nativeWriter, never()).write(same(root));
    }

    @Test
    void testArrowBundleNullabilityMismatchFallsBackToRows() throws Exception {
        RowType writerType = RowType.builder().field("a", DataTypes.INT().notNull()).build();
        RowType bundleType = RowType.builder().field("a", DataTypes.INT()).build();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter);
        VectorSchemaRoot root;

        try (RootAllocator sourceAllocator = new RootAllocator()) {
            root = ArrowUtils.createVectorSchemaRoot(bundleType, sourceAllocator);
            try (VectorSchemaRoot ignored = root) {
                setInt((IntVector) root.getVector("a"), 10);
                root.setRowCount(1);

                writer.writeBundle(new ArrowBundleRecords(root, bundleType, true));

                verify(nativeWriter, never()).write(same(root));
                assertThat(writer.genericBundleRows()).isEqualTo(1);
            }
        } finally {
            writer.close();
        }

        verify(nativeWriter).write(any(VectorSchemaRoot.class));
        verify(nativeWriter, never()).write(same(root));
    }

    @Test
    void testArrowBundleFieldIdMismatchFallsBackToRows() throws Exception {
        RowType writerType =
                new RowType(Collections.singletonList(new DataField(7, "a", DataTypes.INT())));
        RowType bundleType =
                new RowType(Collections.singletonList(new DataField(8, "a", DataTypes.INT())));
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        MosaicRecordsWriter writer = createWriter(writerType, nativeWriter);

        try (RootAllocator sourceAllocator = new RootAllocator();
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(bundleType, sourceAllocator)) {
            setInt((IntVector) root.getVector("a"), 10);
            root.setRowCount(1);

            writer.writeBundle(new ArrowBundleRecords(root, bundleType, true));
            verify(nativeWriter, never()).write(same(root));
            assertThat(writer.genericBundleRows()).isEqualTo(1);
        } finally {
            writer.close();
        }
        verify(nativeWriter).write(any(VectorSchemaRoot.class));
    }

    private static MosaicRecordsWriter createWriter(RowType rowType, MosaicWriter nativeWriter) {
        return createWriter(rowType, nativeWriter, new RootAllocator());
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType, MosaicWriter nativeWriter, BufferAllocator allocator) {
        return createWriter(rowType, nativeWriter, FORMAT_CONTEXT, allocator);
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType,
            MosaicWriter nativeWriter,
            FileFormatFactory.FormatContext formatContext) {
        return createWriter(rowType, nativeWriter, formatContext, new RootAllocator());
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType,
            MosaicWriter nativeWriter,
            FileFormatFactory.FormatContext formatContext,
            BufferAllocator allocator) {
        return new MosaicRecordsWriter(
                new ByteArrayOutputStream(),
                rowType,
                formatContext,
                Collections.emptyList(),
                null,
                allocator,
                (outputStream, arrowSchema, options, bufferAllocator) -> nativeWriter);
    }

    private static void setInt(IntVector vector, int value) {
        vector.allocateNew(1);
        vector.setSafe(0, value);
        vector.setValueCount(1);
    }

    private static HeapIntVector heapInts(int... values) {
        HeapIntVector vector = new HeapIntVector(values.length);
        for (int i = 0; i < values.length; i++) {
            vector.setInt(i, values[i]);
        }
        return vector;
    }

    private static List<Integer> toInts(InternalArray array) {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            values.add(array.getInt(i));
        }
        return values;
    }

    private static class CloseCountingRootAllocator extends RootAllocator {

        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            super.close();
        }

        int closeCount() {
            return closeCount;
        }
    }
}

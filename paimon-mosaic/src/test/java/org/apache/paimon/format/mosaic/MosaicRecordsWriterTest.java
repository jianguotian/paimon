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
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
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
        return new MosaicRecordsWriter(
                new ByteArrayOutputStream(),
                rowType,
                FORMAT_CONTEXT,
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

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

package org.apache.paimon.format.mosaic.extension;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.mosaic.MosaicRecordsWriter;
import org.apache.paimon.format.mosaic.MosaicWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Verifies that a writer outside the Mosaic package can reuse the public lifecycle. */
class MosaicWriterExtensionTest {

    private static final RowType TYPE = RowType.of(DataTypes.INT());
    private static final FileFormatFactory.FormatContext CONTEXT =
            new FileFormatFactory.FormatContext(new Options(), 1024, 1024);

    @Test
    void externalWriterReusesConversionBufferAndCleanup() throws Exception {
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator allocator = new RootAllocator();
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot root = invocation.getArgument(0);
                            assertThat(root.getRowCount()).isEqualTo(2);
                            assertThat(root.getVector(0).getObject(0)).isEqualTo(11);
                            assertThat(root.getVector(0).getObject(1)).isEqualTo(22);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));
        try (ExternalWriter writer =
                new ExternalWriter(mock(OutputStream.class), allocator, nativeWriter)) {
            HeapIntVector input = new HeapIntVector(2);
            input.setInt(0, 11);
            input.setInt(1, 22);
            writer.writeColumns(new ColumnVector[] {input}, 2);
        }
        verify(nativeWriter).write(any(VectorSchemaRoot.class));
        verify(nativeWriter).close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void factoryHookRetainsCompressionValidation() throws Exception {
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator allocator = new RootAllocator();
        int[] created = {0};
        MosaicWriterFactory factory =
                new MosaicWriterFactory(TYPE, CONTEXT) {
                    @Override
                    protected MosaicRecordsWriter createWriter(
                            OutputStream output,
                            RowType type,
                            FileFormatFactory.FormatContext context,
                            List<String> statsColumns,
                            Integer buckets) {
                        created[0]++;
                        assertThat(type).isEqualTo(TYPE);
                        assertThat(context).isSameAs(CONTEXT);
                        assertThat(statsColumns).isEmpty();
                        assertThat(buckets).isNull();
                        return new ExternalWriter(output, allocator, nativeWriter);
                    }
                };
        PositionOutputStream output = mock(PositionOutputStream.class);
        assertThatThrownBy(() -> factory.create(output, "snappy"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(created[0]).isZero();
        try (FormatWriter writer = factory.create(output, "zstd")) {
            assertThat(writer).isInstanceOf(ExternalWriter.class);
            assertThat(created[0]).isEqualTo(1);
        }
        verify(nativeWriter).close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    private static class ExternalWriter extends MosaicRecordsWriter {

        private ExternalWriter(
                OutputStream output, RootAllocator allocator, MosaicWriter nativeWriter) {
            super(
                    output,
                    TYPE,
                    CONTEXT,
                    Collections.emptyList(),
                    null,
                    allocator,
                    (stream, schema, options, bufferAllocator) -> nativeWriter);
        }

        private void writeColumns(ColumnVector[] columns, int rows) {
            checkNotFailed();
            arrowWriter().write(columns, null, 0, rows);
            flush();
        }
    }
}

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

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.converter.ArrowBatchConverter;
import org.apache.paimon.arrow.converter.ArrowPerRowBatchConverter;
import org.apache.paimon.arrow.converter.ArrowVectorizedBatchConverter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.deletionvectors.ApplyDeletionFileRecordIterator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MosaicRowTrackingTest {
    @Test
    void synthesizedRowIdsCanBeConsumedThroughExposedVectorCapability() throws Exception {
        checkRowIds(false, false, false);
    }

    @Test
    void synthesizedRowIdsRemainDistinctAfterDeletionSelection() throws Exception {
        checkRowIds(true, false, false);
    }

    @Test
    void synthesizedRowIdsRemainCorrectThroughRowFallback() throws Exception {
        checkRowIds(true, true, false);
    }

    @Test
    void directDecorationDoesNotExposePositionDependentVectors() throws Exception {
        checkRowIds(false, false, true);
        checkRowIds(true, false, true);
    }

    private void checkRowIds(boolean deletion, boolean forceRowFallback, boolean directDecoration)
            throws Exception {
        RowType type =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(SpecialFields.ROW_ID.name(), DataTypes.BIGINT())
                        .build();
        RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(type, allocator);
        root.allocateNew();
        ((IntVector) root.getVector(0)).setSafe(0, 7);
        ((IntVector) root.getVector(0)).setSafe(1, 8);
        ((IntVector) root.getVector(0)).setSafe(2, 9);
        ((BigIntVector) root.getVector(1)).setNull(0);
        ((BigIntVector) root.getVector(1)).setNull(1);
        ((BigIntVector) root.getVector(1)).setNull(2);
        root.setRowCount(3);
        MosaicReader nativeReader = mock(MosaicReader.class);
        when(nativeReader.getSchema()).thenReturn(root.getSchema());
        when(nativeReader.numRowGroups()).thenReturn(1);
        when(nativeReader.rowGroupNumRows(0)).thenReturn(3);
        when(nativeReader.readRowGroup(0, allocator)).thenReturn(root);
        Path path = new Path("file:/tmp/pr9-review");
        MosaicRecordsReader reader =
                new MosaicRecordsReader(
                        mock(MosaicInputFileAdapter.class),
                        0,
                        type,
                        type,
                        null,
                        path,
                        allocator,
                        (file, size, alloc) -> nativeReader);
        try (DataFileRecordReader dataReader =
                        new DataFileRecordReader(
                                type,
                                reader,
                                false,
                                false,
                                new int[] {0, 1},
                                null,
                                null,
                                true,
                                1000L,
                                123L,
                                Collections.singletonMap(SpecialFields.ROW_ID.name(), 1),
                                null,
                                path);
                RootAllocator outputAllocator = new RootAllocator();
                VectorSchemaRoot output =
                        ArrowUtils.createVectorSchemaRoot(type, outputAllocator)) {
            FileRecordIterator<InternalRow> batch =
                    directDecoration
                            ? ((ColumnarRowIterator) reader.readBatch())
                                    .assignRowTracking(
                                            1000L,
                                            123L,
                                            Collections.singletonMap(
                                                    SpecialFields.ROW_ID.name(), 1))
                            : dataReader.readBatch();
            assertThat(batch).isNotInstanceOf(VectorizedRecordIterator.class);
            ArrowBatchConverter converter;
            if (!forceRowFallback && batch instanceof VectorizedRecordIterator) {
                ArrowVectorizedBatchConverter vectorConverter =
                        new ArrowVectorizedBatchConverter(
                                output, ArrowUtils.createArrowFieldWriters(output, type));
                if (deletion) {
                    vectorConverter.reset(
                            new ApplyDeletionFileRecordIterator(batch, pos -> pos == 1));
                } else {
                    vectorConverter.reset((VectorizedRecordIterator) batch);
                }
                converter = vectorConverter;
            } else {
                ArrowPerRowBatchConverter rowConverter =
                        new ArrowPerRowBatchConverter(
                                output, ArrowUtils.createArrowFieldWriters(output, type));
                rowConverter.reset(
                        deletion
                                ? new ApplyDeletionFileRecordIterator(batch, pos -> pos == 1)
                                : batch);
                converter = rowConverter;
            }
            converter.next(1024);
            long[] observed = new long[output.getRowCount()];
            for (int i = 0; i < observed.length; i++) {
                observed[i] = ((BigIntVector) output.getVector(1)).get(i);
            }
            batch.releaseBatch();
            assertThat(observed)
                    .containsExactly(
                            deletion
                                    ? new long[] {1000L, 1002L}
                                    : new long[] {1000L, 1001L, 1002L});
        }
    }
}

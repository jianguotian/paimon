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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongIterator;
import org.apache.paimon.utils.RecyclableIterator;
import org.apache.paimon.utils.VectorMappingUtils;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link RecordReader.RecordIterator} that returns {@link InternalRow}s. The next row is set by
 * {@link ColumnarRow#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow>
        implements BatchColumnarRowIterator {

    private static final int DYNAMIC_BATCH_ROW_COUNT = -1;

    protected final Path filePath;
    @Nullable protected final ColumnarRow row;
    protected final Runnable recycler;
    private final int batchRowCount;

    protected int num;
    protected int index;
    protected int returnedPositionIndex;
    protected long returnedPosition;
    protected LongIterator positionIterator;

    public ColumnarRowIterator(Path filePath, ColumnarRow row, @Nullable Runnable recycler) {
        this(filePath, row, DYNAMIC_BATCH_ROW_COUNT, recycler);
    }

    protected ColumnarRowIterator(Path filePath, int batchRowCount, @Nullable Runnable recycler) {
        this(filePath, null, batchRowCount, recycler);
    }

    private ColumnarRowIterator(
            Path filePath,
            @Nullable ColumnarRow row,
            int batchRowCount,
            @Nullable Runnable recycler) {
        super(recycler);
        this.filePath = filePath;
        this.row = row;
        this.recycler = recycler;
        this.batchRowCount = batchRowCount;
    }

    public void reset(long nextFilePos) {
        int rowCount = batchRowCount();
        reset(LongIterator.fromRange(nextFilePos, nextFilePos + rowCount));
    }

    public void reset(LongIterator positions) {
        this.positionIterator = positions;
        this.num = batchRowCount();
        this.index = 0;
        this.returnedPositionIndex = 0;
        this.returnedPosition = -1;
    }

    @Nullable
    @Override
    public InternalRow next() {
        if (index < num) {
            ColumnarRow currentRow = row();
            currentRow.setRowId(index++);
            return currentRow;
        } else {
            return null;
        }
    }

    @Override
    public long returnedPosition() {
        for (int i = 0; i < index - returnedPositionIndex; i++) {
            returnedPosition = positionIterator.next();
        }
        returnedPositionIndex = index;
        if (returnedPosition == -1) {
            throw new IllegalStateException("returnedPosition() is called before next()");
        }

        return returnedPosition;
    }

    @Override
    public Path filePath() {
        return this.filePath;
    }

    @Override
    public VectorizedColumnBatch batch() {
        return row().batch();
    }

    @Override
    public ColumnarRowIterator copy(ColumnVector[] vectors) {
        // We should call copy only when the iterator is at the beginning of the file.
        checkArgument(returnedPositionIndex == 0, "copy() should not be called after next()");
        ColumnarRowIterator newIterator =
                new ColumnarRowIterator(filePath, row().copy(vectors), recycler);
        newIterator.reset(positionIterator);
        return newIterator;
    }

    public ColumnarRowIterator mapping(
            @Nullable PartitionInfo partitionInfo, @Nullable int[] indexMapping) {
        if (partitionInfo != null || indexMapping != null) {
            VectorizedColumnBatch vectorizedColumnBatch = row().batch();
            ColumnVector[] vectors = vectorizedColumnBatch.columns;
            if (partitionInfo != null) {
                vectors = VectorMappingUtils.createPartitionMappedVectors(partitionInfo, vectors);
            }
            if (indexMapping != null) {
                vectors = VectorMappingUtils.createMappedVectors(indexMapping, vectors);
            }
            return copy(vectors);
        }
        return this;
    }

    /**
     * Maps this batch to the final output row type.
     *
     * <p>Subclasses which can preserve a native batch representation may override this overload to
     * use the final row names and types. The default implementation keeps the existing mapping
     * behavior.
     */
    public ColumnarRowIterator mapping(
            RowType outputRowType,
            @Nullable PartitionInfo partitionInfo,
            @Nullable int[] indexMapping) {
        return mapping(partitionInfo, indexMapping);
    }

    public ColumnarRowIterator assignRowTracking(
            Long firstRowId, Long snapshotId, Map<String, Integer> meta) {
        VectorizedColumnBatch vectorizedColumnBatch = row().batch();
        ColumnVector[] vectors = vectorizedColumnBatch.columns;

        if (meta.containsKey(SpecialFields.ROW_ID.name())) {
            Integer index = meta.get(SpecialFields.ROW_ID.name());
            final ColumnVector rowIdVector = vectors[index];
            vectors[index] =
                    new LongColumnVector() {
                        @Override
                        public long getLong(int i) {
                            if (rowIdVector.isNullAt(i)) {
                                return firstRowId + returnedPosition();
                            } else {
                                return ((LongColumnVector) rowIdVector).getLong(i);
                            }
                        }

                        @Override
                        public boolean isNullAt(int i) {
                            return false;
                        }
                    };
        }

        if (meta.containsKey(SpecialFields.SEQUENCE_NUMBER.name())) {
            Integer index = meta.get(SpecialFields.SEQUENCE_NUMBER.name());
            final ColumnVector versionVector = vectors[index];
            vectors[index] =
                    new LongColumnVector() {
                        @Override
                        public long getLong(int i) {
                            if (versionVector.isNullAt(i)) {
                                return snapshotId;
                            } else {
                                return ((LongColumnVector) versionVector).getLong(i);
                            }
                        }

                        @Override
                        public boolean isNullAt(int i) {
                            return false;
                        }
                    };
        }

        copy(vectors);
        return this;
    }

    protected ColumnarRow row() {
        if (row == null) {
            throw new IllegalStateException("Columnar row has not been initialized.");
        }
        return row;
    }

    private int batchRowCount() {
        return batchRowCount == DYNAMIC_BATCH_ROW_COUNT
                ? row().batch().getNumRows()
                : batchRowCount;
    }
}

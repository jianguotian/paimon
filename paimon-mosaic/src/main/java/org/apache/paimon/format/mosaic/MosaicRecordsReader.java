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
import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.arrow.reader.ArrowVectorizedRecordIterator;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.VectorizedRowIterator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.format.mosaic.MosaicObjects.convertStatsValue;

/** File reader for Mosaic format. */
public class MosaicRecordsReader implements FileRecordReader<InternalRow> {

    private final MosaicInputFileAdapter inputFileAdapter;
    private final MosaicReader reader;
    private final ArrowBatchReader arrowBatchReader;
    private final Path filePath;
    private final BufferAllocator allocator;
    private final int numRowGroups;
    private final RowType dataSchemaRowType;
    private final RowType projectedRowType;
    private final int projectedFieldCount;
    private final boolean allProjectedColumnsMissing;
    private final boolean arrowBundleCompatible;
    @Nullable private final List<Predicate> predicates;

    private int currentRowGroup;
    private long returnedPosition = -1;
    private final List<BatchRecycler> activeBatches = new ArrayList<>();

    public MosaicRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath) {
        this(
                inputFileAdapter,
                fileSize,
                dataSchemaRowType,
                projectedRowType,
                predicates,
                filePath,
                new RootAllocator(),
                MosaicReader::open);
    }

    MosaicRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath,
            BufferAllocator allocator,
            NativeReaderOpener nativeReaderOpener) {
        this.filePath = filePath;
        this.inputFileAdapter = inputFileAdapter;
        this.dataSchemaRowType = dataSchemaRowType;
        this.projectedRowType = projectedRowType;
        this.projectedFieldCount = projectedRowType.getFieldCount();
        this.predicates = predicates;
        this.allocator = allocator;

        MosaicReader createdReader = null;
        int createdNumRowGroups;
        ArrowBatchReader createdArrowBatchReader;
        boolean createdAllProjectedColumnsMissing = false;
        try {
            createdReader = nativeReaderOpener.open(inputFileAdapter, fileSize, allocator);

            Schema fileSchema = createdReader.getSchema();
            Set<String> fileColumnNames = new HashSet<>();
            for (Field field : fileSchema.getFields()) {
                fileColumnNames.add(field.getName());
            }
            List<String> projectedNames = projectedRowType.getFieldNames();
            List<String> existingColumns = new ArrayList<>();
            for (String name : projectedNames) {
                if (fileColumnNames.contains(name)) {
                    existingColumns.add(name);
                }
            }
            createdAllProjectedColumnsMissing = existingColumns.isEmpty();
            this.arrowBundleCompatible =
                    existingColumns.size() == projectedFieldCount
                            && MosaicArrowSchemaCompatibility.matchesProjection(
                                    projectedRowType, fileSchema);
            if (!existingColumns.isEmpty()
                    && !hasExactProjection(projectedNames, fileSchema.getFields())) {
                createdReader.project(existingColumns.toArray(new String[0]));
            }

            createdNumRowGroups = createdReader.numRowGroups();
            createdArrowBatchReader = new ArrowBatchReader(projectedRowType, true);
        } catch (Throwable t) {
            closeOnConstructionFailure(t, createdReader, allocator, inputFileAdapter);
            throw rethrowUnchecked(t);
        }

        this.reader = createdReader;
        this.numRowGroups = createdNumRowGroups;
        this.allProjectedColumnsMissing = createdAllProjectedColumnsMissing;
        this.currentRowGroup = 0;
        this.arrowBatchReader = createdArrowBatchReader;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        while (currentRowGroup < numRowGroups) {
            int numRows = reader.rowGroupNumRows(currentRowGroup);
            if (!matchesRowGroup(currentRowGroup, numRows)) {
                returnedPosition += numRows;
                currentRowGroup++;
                continue;
            }

            if (allProjectedColumnsMissing) {
                currentRowGroup++;
                long batchStartPosition = returnedPosition + 1;
                returnedPosition += numRows;
                return allNullIterator(batchStartPosition, numRows);
            }

            VectorSchemaRoot vsr = reader.readRowGroup(currentRowGroup, allocator);
            currentRowGroup++;

            long batchStartPosition = returnedPosition + 1;
            returnedPosition += vsr.getRowCount();
            BatchRecycler recycler = recycler(vsr);
            try {
                if (arrowBundleCompatible) {
                    return new MosaicArrowVectorizedRecordIterator(
                            filePath,
                            batchStartPosition,
                            projectedRowType,
                            arrowBatchReader,
                            vsr,
                            recycler);
                }

                VectorizedColumnBatch batch = arrowBatchReader.readVectorizedBatch(vsr);
                VectorizedRowIterator iterator =
                        new VectorizedRowIterator(filePath, new ColumnarRow(batch), recycler);
                iterator.reset(batchStartPosition);
                return iterator;
            } catch (Throwable t) {
                try {
                    recycler.run();
                } catch (Throwable closeFailure) {
                    t.addSuppressed(closeFailure);
                }
                rethrow(t);
            }
        }
        return null;
    }

    private FileRecordIterator<InternalRow> allNullIterator(long batchStartPosition, int numRows) {
        GenericRow row = new GenericRow(projectedFieldCount);
        return new FileRecordIterator<InternalRow>() {
            private int position;
            private long batchReturnedPosition = -1;

            @Override
            public long returnedPosition() {
                if (batchReturnedPosition < 0) {
                    throw new IllegalStateException("returnedPosition() is called before next()");
                }
                return batchReturnedPosition;
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next() {
                if (position < numRows) {
                    batchReturnedPosition = batchStartPosition + position;
                    position++;
                    return row;
                }
                return null;
            }

            @Override
            public void releaseBatch() {}
        };
    }

    private boolean matchesRowGroup(int rowGroupIndex, long rowCount) {
        if (predicates == null || predicates.isEmpty()) {
            return true;
        }

        Map<String, ColumnStatistics> statsMap = reader.getRowGroupStatistics(rowGroupIndex);
        if (statsMap.isEmpty()) {
            return true;
        }

        int fieldCount = dataSchemaRowType.getFieldCount();
        GenericRow minValues = new GenericRow(fieldCount);
        GenericRow maxValues = new GenericRow(fieldCount);
        long[] nullCounts = new long[fieldCount];

        List<DataField> fields = dataSchemaRowType.getFields();
        for (int i = 0; i < fieldCount; i++) {
            String colName = fields.get(i).name();
            ColumnStatistics stats = statsMap.get(colName);
            if (stats == null) {
                continue;
            }

            nullCounts[i] = stats.getNullCount();
            if (stats.hasMinMax()) {
                DataType dataType = fields.get(i).type();
                Object min = convertStatsValue(stats.getMin(), dataType);
                Object max = convertStatsValue(stats.getMax(), dataType);
                minValues.setField(i, min);
                maxValues.setField(i, max);
            }
        }

        for (Predicate predicate : predicates) {
            if (!predicate.test(rowCount, minValues, maxValues, new GenericArray(nullCounts))) {
                return false;
            }
        }
        return true;
    }

    private BatchRecycler recycler(VectorSchemaRoot vsr) {
        BatchRecycler recycler = new BatchRecycler(vsr);
        activeBatches.add(recycler);
        return recycler;
    }

    private static boolean hasExactProjection(List<String> projectedNames, List<Field> fileFields) {
        if (projectedNames.size() != fileFields.size()) {
            return false;
        }
        for (int i = 0; i < projectedNames.size(); i++) {
            if (!projectedNames.get(i).equals(fileFields.get(i).getName())) {
                return false;
            }
        }
        return true;
    }

    private void releaseActiveBatches() {
        Throwable failure = null;
        for (BatchRecycler recycler : new ArrayList<>(activeBatches)) {
            try {
                recycler.run();
            } catch (Throwable t) {
                failure = addSuppressed(failure, t);
            }
        }
        if (failure != null) {
            throw rethrowUnchecked(failure);
        }
    }

    @Override
    public void close() throws IOException {
        Throwable throwable = null;

        try {
            releaseActiveBatches();
        } catch (Throwable t) {
            throwable = t;
        }

        try {
            reader.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            inputFileAdapter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        if (throwable != null) {
            rethrow(throwable);
        }
    }

    private static Throwable addSuppressed(Throwable throwable, Throwable suppressed) {
        if (throwable == null) {
            return suppressed;
        }
        throwable.addSuppressed(suppressed);
        return throwable;
    }

    private static void rethrow(Throwable throwable) throws IOException {
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new IOException(throwable);
    }

    private static RuntimeException rethrowUnchecked(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        return new RuntimeException(throwable);
    }

    private static void closeOnConstructionFailure(
            Throwable throwable,
            @Nullable MosaicReader reader,
            BufferAllocator allocator,
            MosaicInputFileAdapter inputFileAdapter) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            inputFileAdapter.close();
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }
    }

    @FunctionalInterface
    interface NativeReaderOpener {

        MosaicReader open(
                MosaicInputFileAdapter inputFileAdapter, long fileSize, BufferAllocator allocator);
    }

    private class BatchRecycler implements Runnable {

        private final VectorSchemaRoot vsr;
        private final AtomicBoolean released = new AtomicBoolean();

        private BatchRecycler(VectorSchemaRoot vsr) {
            this.vsr = vsr;
        }

        @Override
        public void run() {
            if (!released.compareAndSet(false, true)) {
                return;
            }

            activeBatches.remove(this);
            vsr.close();
        }
    }

    private static class MosaicArrowVectorizedRecordIterator extends VectorizedRowIterator
            implements ArrowVectorizedRecordIterator {

        private final RowType arrowRowType;
        private final VectorSchemaRoot vsr;
        private final BatchRecycler batchRecycler;
        private final long startPosition;

        private MosaicArrowVectorizedRecordIterator(
                Path filePath,
                long startPosition,
                RowType arrowRowType,
                ArrowBatchReader arrowBatchReader,
                VectorSchemaRoot vsr,
                BatchRecycler recycler) {
            super(filePath, new ColumnarRow(arrowBatchReader.readVectorizedBatch(vsr)), recycler);
            this.arrowRowType = arrowRowType;
            this.vsr = vsr;
            this.batchRecycler = recycler;
            this.startPosition = startPosition;
            reset(startPosition);
        }

        @Override
        public ArrowBundleRecords arrowBundle() {
            return new MosaicArrowBundleRecords(vsr, arrowRowType);
        }

        @Override
        public VectorizedRowIterator copy(ColumnVector[] vectors) {
            ColumnVector[] current = batch().columns;
            if (current.length == vectors.length) {
                boolean identityMapping = true;
                for (int i = 0; i < current.length; i++) {
                    if (current[i] != vectors[i]) {
                        identityMapping = false;
                        break;
                    }
                }
                if (identityMapping) {
                    return this;
                }
            }
            return vectorizedFallback(vectors);
        }

        @Override
        public ColumnarRowIterator assignRowTracking(
                Long firstRowId, Long snapshotId, Map<String, Integer> meta) {
            return vectorizedFallback().assignRowTracking(firstRowId, snapshotId, meta);
        }

        private VectorizedRowIterator vectorizedFallback() {
            return vectorizedFallback(batch().columns.clone());
        }

        private VectorizedRowIterator vectorizedFallback(ColumnVector[] columns) {
            VectorizedColumnBatch copiedBatch = batch().copy(columns);
            VectorizedRowIterator iterator =
                    new VectorizedRowIterator(
                            filePath, new ColumnarRow(copiedBatch), batchRecycler);
            iterator.reset(startPosition);
            return iterator;
        }
    }
}

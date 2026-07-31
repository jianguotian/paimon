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
import org.apache.paimon.arrow.reader.ArrowVectorizedRecordIterator;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.VectorMappingUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
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
import java.util.stream.Collectors;

import static org.apache.paimon.format.mosaic.MosaicObjects.convertStatsValue;

/** File reader for Mosaic format. */
public class MosaicRecordsReader implements FileRecordReader<InternalRow> {

    private static final long MAX_SYNTHESIZED_PARTITION_ARROW_BYTES = 64L * 1024 * 1024;
    private static final long PARTITION_VECTOR_BASE_BYTES = 4096L;
    private static final long PARTITION_VALUE_ROW_OVERHEAD_BYTES = 16L;

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
                            recycler,
                            allocator);
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
        private final List<Runnable> cleanups = new ArrayList<>();

        private BatchRecycler(VectorSchemaRoot vsr) {
            this.vsr = vsr;
        }

        private synchronized void addCleanup(Runnable cleanup) {
            if (released.get()) {
                cleanup.run();
            } else {
                cleanups.add(cleanup);
            }
        }

        @Override
        public void run() {
            List<Runnable> cleanupsToRun;
            synchronized (this) {
                if (!released.compareAndSet(false, true)) {
                    return;
                }
                cleanupsToRun = new ArrayList<>(cleanups);
                cleanups.clear();
            }

            Throwable failure = null;
            for (Runnable cleanup : cleanupsToRun) {
                try {
                    cleanup.run();
                } catch (Throwable t) {
                    failure = addSuppressed(failure, t);
                }
            }

            activeBatches.remove(this);
            try {
                vsr.close();
            } catch (Throwable t) {
                failure = addSuppressed(failure, t);
            }

            if (failure != null) {
                throw rethrowUnchecked(failure);
            }
        }
    }

    private static class MosaicArrowVectorizedRecordIterator extends VectorizedRowIterator
            implements ArrowVectorizedRecordIterator {

        private final RowType arrowRowType;
        private final VectorSchemaRoot vsr;
        private final BatchRecycler batchRecycler;
        private final BufferAllocator allocator;
        private final long startPosition;

        private MosaicArrowVectorizedRecordIterator(
                Path filePath,
                long startPosition,
                RowType arrowRowType,
                ArrowBatchReader arrowBatchReader,
                VectorSchemaRoot vsr,
                BatchRecycler recycler,
                BufferAllocator allocator) {
            this(
                    filePath,
                    startPosition,
                    arrowRowType,
                    arrowBatchReader.readVectorizedBatch(vsr),
                    vsr,
                    recycler,
                    allocator);
        }

        private MosaicArrowVectorizedRecordIterator(
                Path filePath,
                long startPosition,
                RowType arrowRowType,
                VectorizedColumnBatch vectorizedBatch,
                VectorSchemaRoot vsr,
                BatchRecycler recycler,
                BufferAllocator allocator) {
            super(filePath, new ColumnarRow(vectorizedBatch), recycler);
            this.arrowRowType = arrowRowType;
            this.vsr = vsr;
            this.batchRecycler = recycler;
            this.allocator = allocator;
            this.startPosition = startPosition;
            reset(startPosition);
        }

        @Override
        public ArrowBundleRecords arrowBundle() {
            return new MosaicArrowBundleRecords(vsr, arrowRowType);
        }

        @Override
        public ColumnarRowIterator mapping(
                RowType outputRowType,
                @Nullable PartitionInfo partitionInfo,
                @Nullable int[] indexMapping) {
            if (isIdentityMapping(indexMapping, outputRowType.getFieldCount())) {
                indexMapping = null;
            }
            if (partitionInfo == null && indexMapping == null) {
                return sameFields(outputRowType, arrowRowType) ? this : vectorizedFallback();
            }
            if (!canPreserveArrowBundle(outputRowType, partitionInfo, indexMapping)) {
                return super.mapping(partitionInfo, indexMapping);
            }

            return new PartitionMappedMosaicArrowVectorizedRecordIterator(
                    filePath,
                    startPosition,
                    outputRowType,
                    batch(),
                    vsr,
                    batchRecycler,
                    allocator,
                    partitionInfo);
        }

        private boolean canPreserveArrowBundle(
                RowType outputRowType,
                @Nullable PartitionInfo partitionInfo,
                @Nullable int[] indexMapping) {
            if (partitionInfo == null
                    || indexMapping != null
                    || partitionInfo.size() != outputRowType.getFieldCount()) {
                return false;
            }

            boolean[] referencedPhysicalFields = new boolean[arrowRowType.getFieldCount()];
            for (int i = 0; i < partitionInfo.size(); i++) {
                DataField logicalField = outputRowType.getFields().get(i);
                if (partitionInfo.inPartitionRow(i)) {
                    if (!logicalField.type().equalsIgnoreFieldId(partitionInfo.getType(i))) {
                        return false;
                    }
                    try {
                        logicalField.type().accept(ArrowFieldWriterFactoryVisitor.INSTANCE);
                    } catch (UnsupportedOperationException e) {
                        return false;
                    }
                    continue;
                }

                int physicalIndex = partitionInfo.getRealIndex(i);
                if (physicalIndex < 0 || physicalIndex >= arrowRowType.getFieldCount()) {
                    return false;
                }
                DataField physicalField = arrowRowType.getFields().get(physicalIndex);
                if (!logicalField.name().equals(physicalField.name())
                        || !logicalField.type().equalsIgnoreFieldId(physicalField.type())
                        || referencedPhysicalFields[physicalIndex]) {
                    return false;
                }
                referencedPhysicalFields[physicalIndex] = true;
            }

            for (boolean referenced : referencedPhysicalFields) {
                if (!referenced) {
                    return false;
                }
            }
            return estimateSynthesizedPartitionBytes(
                            outputRowType, partitionInfo, vsr.getRowCount(), allocator)
                    <= MAX_SYNTHESIZED_PARTITION_ARROW_BYTES;
        }

        @Override
        public ColumnarRowIterator assignRowTracking(
                Long firstRowId, Long snapshotId, Map<String, Integer> meta) {
            return vectorizedFallback().assignRowTracking(firstRowId, snapshotId, meta);
        }

        private VectorizedRowIterator vectorizedFallback() {
            ColumnVector[] columns = batch().columns.clone();
            VectorizedColumnBatch copiedBatch = batch().copy(columns);
            VectorizedRowIterator iterator =
                    new VectorizedRowIterator(
                            filePath, new ColumnarRow(copiedBatch), batchRecycler);
            iterator.reset(startPosition);
            return iterator;
        }

        private static boolean sameFields(RowType left, RowType right) {
            if (left.getFieldCount() != right.getFieldCount()) {
                return false;
            }
            for (int i = 0; i < left.getFieldCount(); i++) {
                DataField leftField = left.getFields().get(i);
                DataField rightField = right.getFields().get(i);
                if (!leftField.name().equals(rightField.name())
                        || !leftField.type().equalsIgnoreFieldId(rightField.type())) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isIdentityMapping(@Nullable int[] mapping, int fieldCount) {
            if (mapping == null || mapping.length != fieldCount) {
                return false;
            }
            for (int i = 0; i < mapping.length; i++) {
                if (mapping[i] != i) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class PartitionMappedMosaicArrowVectorizedRecordIterator
            extends MosaicArrowVectorizedRecordIterator {

        private final RowType rowType;
        private final VectorSchemaRoot physicalVsr;
        private final BufferAllocator allocator;
        private final PartitionInfo partitionInfo;
        private final List<FieldVector> ownedPartitionVectors = new ArrayList<>();

        @Nullable private VectorSchemaRoot mappedVsr;
        @Nullable private BufferAllocator partitionAllocator;

        private PartitionMappedMosaicArrowVectorizedRecordIterator(
                Path filePath,
                long startPosition,
                RowType rowType,
                VectorizedColumnBatch physicalBatch,
                VectorSchemaRoot physicalVsr,
                BatchRecycler recycler,
                BufferAllocator allocator,
                PartitionInfo partitionInfo) {
            super(
                    filePath,
                    startPosition,
                    rowType,
                    createMappedBatch(physicalBatch, partitionInfo),
                    physicalVsr,
                    recycler,
                    allocator);
            this.rowType = rowType;
            this.physicalVsr = physicalVsr;
            this.allocator = allocator;
            this.partitionInfo = partitionInfo;
            recycler.addCleanup(this::closePartitionResources);
        }

        private static VectorizedColumnBatch createMappedBatch(
                VectorizedColumnBatch physicalBatch, PartitionInfo partitionInfo) {
            ColumnVector[] mappedColumns =
                    VectorMappingUtils.createPartitionMappedVectors(
                            partitionInfo, physicalBatch.columns);
            return physicalBatch.copy(mappedColumns);
        }

        @Override
        public ArrowBundleRecords arrowBundle() {
            if (mappedVsr == null) {
                mappedVsr = createMappedVsr();
            }
            return new MosaicArrowBundleRecords(mappedVsr, rowType);
        }

        private VectorSchemaRoot createMappedVsr() {
            List<Field> fields =
                    rowType.getFields().stream()
                            .map(
                                    field ->
                                            ArrowUtils.toArrowField(
                                                    field.name(), field.id(), field.type(), 0))
                            .collect(Collectors.toList());
            List<FieldVector> vectors = new ArrayList<>(fields.size());
            try {
                for (int i = 0; i < fields.size(); i++) {
                    if (partitionInfo.inPartitionRow(i)) {
                        FieldVector vector = createPartitionVector(i);
                        ownedPartitionVectors.add(vector);
                        vectors.add(vector);
                    } else {
                        vectors.add(physicalVsr.getVector(partitionInfo.getRealIndex(i)));
                    }
                }
                return new VectorSchemaRoot(fields, vectors, physicalVsr.getRowCount());
            } catch (Throwable t) {
                try {
                    closeOwnedPartitionVectors();
                } catch (Throwable closeFailure) {
                    t.addSuppressed(closeFailure);
                }
                throw t;
            }
        }

        private FieldVector createPartitionVector(int logicalIndex) {
            DataField field = rowType.getFields().get(logicalIndex);
            FieldVector vector = ArrowUtils.createVector(field, partitionAllocator(), true);
            try {
                int rowCount = physicalVsr.getRowCount();
                BinaryRow partition = partitionInfo.getPartitionRow();
                int partitionIndex = partitionInfo.getRealIndex(logicalIndex);
                if (vector instanceof BaseVariableWidthVector) {
                    long dataBytes =
                            saturatedMultiply(
                                    partitionValueBytes(field.type(), partition, partitionIndex),
                                    rowCount);
                    ((BaseVariableWidthVector) vector).allocateNew(dataBytes, rowCount);
                } else {
                    vector.setInitialCapacity(rowCount);
                    vector.allocateNew();
                }

                ArrowFieldWriter writer =
                        field.type()
                                .accept(ArrowFieldWriterFactoryVisitor.INSTANCE)
                                .create(vector, field.type().isNullable());
                for (int i = 0; i < rowCount; i++) {
                    writer.write(i, partition, partitionIndex);
                }
                vector.setValueCount(rowCount);
                return vector;
            } catch (Throwable t) {
                try {
                    vector.close();
                } catch (Throwable closeFailure) {
                    t.addSuppressed(closeFailure);
                }
                throw t;
            }
        }

        private void closeOwnedPartitionVectors() {
            Throwable failure = null;
            for (FieldVector vector : ownedPartitionVectors) {
                try {
                    vector.close();
                } catch (Throwable t) {
                    failure = addSuppressed(failure, t);
                }
            }
            ownedPartitionVectors.clear();
            if (failure != null) {
                throw rethrowUnchecked(failure);
            }
        }

        private BufferAllocator partitionAllocator() {
            if (partitionAllocator == null) {
                partitionAllocator =
                        allocator.newChildAllocator(
                                "mosaic-partition-mapping",
                                0,
                                MAX_SYNTHESIZED_PARTITION_ARROW_BYTES);
            }
            return partitionAllocator;
        }

        private void closePartitionResources() {
            Throwable failure = null;
            try {
                closeOwnedPartitionVectors();
            } catch (Throwable t) {
                failure = t;
            }
            if (partitionAllocator != null) {
                try {
                    partitionAllocator.close();
                } catch (Throwable t) {
                    failure = addSuppressed(failure, t);
                } finally {
                    partitionAllocator = null;
                }
            }
            if (failure != null) {
                throw rethrowUnchecked(failure);
            }
        }
    }

    private static long estimateSynthesizedPartitionBytes(
            RowType outputRowType,
            PartitionInfo partitionInfo,
            int rowCount,
            BufferAllocator allocator) {
        long total = 0;
        BinaryRow partition = partitionInfo.getPartitionRow();
        for (int i = 0; i < partitionInfo.size(); i++) {
            if (!partitionInfo.inPartitionRow(i)) {
                continue;
            }

            DataType type = outputRowType.getFields().get(i).type();
            int partitionIndex = partitionInfo.getRealIndex(i);
            long valueBytes = partitionValueBytes(type, partition, partitionIndex);
            if (valueBytes == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }

            DataTypeRoot root = type.getTypeRoot();
            long dataBytes = saturatedMultiply(valueBytes, rowCount);
            long vectorBytes =
                    saturatedAdd(
                            PARTITION_VECTOR_BASE_BYTES,
                            roundedAllocationBytes(allocator, dataBytes));
            vectorBytes =
                    saturatedAdd(
                            vectorBytes, roundedAllocationBytes(allocator, (rowCount + 7L) / 8L));
            if (isVariableWidth(root)) {
                vectorBytes =
                        saturatedAdd(
                                vectorBytes,
                                roundedAllocationBytes(
                                        allocator, saturatedMultiply(rowCount + 1L, 4L)));
            } else {
                vectorBytes =
                        saturatedAdd(
                                vectorBytes,
                                saturatedMultiply(PARTITION_VALUE_ROW_OVERHEAD_BYTES, rowCount));
            }
            total = saturatedAdd(total, vectorBytes);
            if (total > MAX_SYNTHESIZED_PARTITION_ARROW_BYTES) {
                return total;
            }
        }
        return total;
    }

    private static boolean isVariableWidth(DataTypeRoot root) {
        return root == DataTypeRoot.CHAR
                || root == DataTypeRoot.VARCHAR
                || root == DataTypeRoot.BINARY
                || root == DataTypeRoot.VARBINARY;
    }

    private static long roundedAllocationBytes(BufferAllocator allocator, long bytes) {
        if (bytes == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return allocator.getRoundingPolicy().getRoundedSize(bytes);
    }

    private static long partitionValueBytes(DataType type, BinaryRow partition, int index) {
        if (partition.isNullAt(index)) {
            return 1;
        }

        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case CHAR:
            case VARCHAR:
                return partition.getString(index).getSizeInBytes();
            case BOOLEAN:
            case TINYINT:
                return 1;
            case BINARY:
            case VARBINARY:
                return partition.getBinary(index).length;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 4;
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 8;
            case DECIMAL:
                return 32;
            case VARIANT:
            case BLOB:
            case ARRAY:
            case VECTOR:
            case MULTISET:
            case MAP:
            case ROW:
                return Long.MAX_VALUE;
            default:
                throw new IllegalArgumentException("Unsupported partition type root: " + root);
        }
    }

    private static long saturatedAdd(long left, long right) {
        if (left == Long.MAX_VALUE || right == Long.MAX_VALUE || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long saturatedMultiply(long left, long right) {
        if (left == 0 || right == 0) {
            return 0;
        }
        if (left == Long.MAX_VALUE || right == Long.MAX_VALUE || left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }
}

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
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.VectorizedBundleRecords;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.mosaic.WriterOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.OversizedAllocationException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Mosaic records writer. */
public class MosaicRecordsWriter implements BundleFormatWriter {

    private static final int MEMORY_CHECK_INTERVAL = 32;

    private final ArrowFormatWriter arrowFormatWriter;
    private final MosaicWriter nativeWriter;
    private final BufferAllocator allocator;
    private final List<String> statsColumnNames;
    private final RowType rowType;
    private final Schema arrowSchema;
    private final long writeBatchMemory;
    private final int initialVectorBatchRows;
    @Nullable private RowType verifiedDirectRowType;
    @Nullable private Schema verifiedDirectSchema;
    private long directArrowRows;
    private long mosaicBundleFallbackRows;
    private long genericBundleRows;
    private boolean failed;
    @Nullable private MosaicWriterMetadata metadata;

    public MosaicRecordsWriter(
            OutputStream outputStream,
            RowType rowType,
            FileFormatFactory.FormatContext formatContext,
            List<String> statsColumnNames,
            @Nullable Integer numBuckets) {
        this(
                outputStream,
                rowType,
                formatContext,
                statsColumnNames,
                numBuckets,
                new RootAllocator(),
                MosaicWriter::new);
    }

    MosaicRecordsWriter(
            OutputStream outputStream,
            RowType rowType,
            FileFormatFactory.FormatContext formatContext,
            List<String> statsColumnNames,
            @Nullable Integer numBuckets,
            BufferAllocator allocator,
            NativeWriterFactory nativeWriterFactory) {
        this.statsColumnNames = statsColumnNames;
        this.rowType = rowType;
        this.allocator = allocator;

        int writeBatchSize = formatContext.writeBatchSize();
        long writeBatchMemory = formatContext.writeBatchMemory().getBytes();
        this.writeBatchMemory = writeBatchMemory;
        this.initialVectorBatchRows =
                initialVectorBatchRows(rowType, writeBatchSize, writeBatchMemory);

        WriterOptions options = new WriterOptions().zstdLevel(formatContext.zstdLevel());
        if (numBuckets != null) {
            options = options.numBuckets(numBuckets);
        }
        MemorySize blockSize = formatContext.blockSize();
        if (blockSize != null) {
            options = options.rowGroupMaxSize(blockSize.getBytes());
        }
        if (!statsColumnNames.isEmpty()) {
            options.statsColumns(statsColumnNames.toArray(new String[0]));
        }

        ArrowFormatWriter createdArrowWriter = null;
        MosaicWriter createdNativeWriter = null;
        Schema createdArrowSchema;
        try {
            checkArgument(
                    writeBatchSize > 0,
                    "'write.batch-size' must be greater than 0, but was %s.",
                    writeBatchSize);
            createdArrowWriter =
                    ArrowFormatWriter.forBorrowedAllocator(
                            rowType, writeBatchSize, true, allocator, writeBatchMemory);
            createdArrowSchema = createdArrowWriter.getVectorSchemaRoot().getSchema();
            createdNativeWriter =
                    nativeWriterFactory.create(
                            outputStream, createdArrowSchema, options, allocator);
        } catch (Throwable t) {
            closeOnConstructionFailure(t, createdNativeWriter, createdArrowWriter, allocator);
            throw rethrowUnchecked(t);
        }

        this.arrowFormatWriter = createdArrowWriter;
        this.nativeWriter = createdNativeWriter;
        this.arrowSchema = createdArrowSchema;
    }

    @Override
    public void addElement(InternalRow internalRow) {
        checkNotFailed();
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Failed to write row to Mosaic file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) {
        checkNotFailed();
        if (bundleRecords instanceof ArrowBundleRecords) {
            ArrowBundleRecords arrowBundle = (ArrowBundleRecords) bundleRecords;
            VectorSchemaRoot root = arrowBundle.getVectorSchemaRoot();
            RowType bundleRowType = arrowBundle.getRowType();
            Schema bundleSchema = root.getSchema();
            boolean trustedMosaicBundle = arrowBundle instanceof MosaicArrowBundleRecords;
            boolean schemaCompatible =
                    trustedMosaicBundle
                            && bundleRowType == verifiedDirectRowType
                            && bundleSchema.equals(verifiedDirectSchema);
            if (!schemaCompatible) {
                schemaCompatible =
                        trustedMosaicBundle
                                ? MosaicArrowSchemaCompatibility.matchesRowType(
                                                rowType, bundleRowType)
                                        && MosaicArrowSchemaCompatibility.matchesWriter(
                                                arrowSchema, bundleSchema)
                                : arrowFormatWriter.isArrowBundleSchemaCompatible(arrowBundle);
                if (schemaCompatible && trustedMosaicBundle) {
                    verifiedDirectRowType = bundleRowType;
                    verifiedDirectSchema = bundleSchema;
                }
            }
            if (schemaCompatible && hasSingleInputAllocatorRoot(root)) {
                writeDirectArrow(root, arrowBundle.rowCount());
                return;
            }
            if (trustedMosaicBundle) {
                mosaicBundleFallbackRows += arrowBundle.rowCount();
            } else {
                genericBundleRows += arrowBundle.rowCount();
            }
            writeRows(arrowBundle);
            return;
        }

        if (bundleRecords instanceof VectorizedBundleRecords) {
            writeVectorizedBundle((VectorizedBundleRecords) bundleRecords);
            return;
        }

        genericBundleRows += bundleRecords.rowCount();
        writeRows(bundleRecords);
    }

    private void writeVectorizedBundle(VectorizedBundleRecords records) {
        flush();

        VectorizedColumnBatch batch = records.batch();
        int[] selected = records.selected();
        int totalRows = selected == null ? batch.getNumRows() : selected.length;
        int batchSize = arrowFormatWriter.getBatchSize();
        int startIndex = 0;
        while (startIndex < totalRows) {
            int batchRows =
                    prepareVectorizedBatch(
                            batch.columns,
                            selected,
                            startIndex,
                            Math.min(batchSize, totalRows - startIndex));
            flush();
            startIndex += batchRows;
        }
    }

    private int prepareVectorizedBatch(
            ColumnVector[] columns, @Nullable int[] selected, int startIndex, int maxBatchRows) {
        // Variable-width data starts with the same 32-row memory-check granularity as row writes.
        // A successful probe may grow before native consumption; an oversized candidate is cleared
        // and retried so its buffers are not retained for the rest of the compaction.
        int batchRows = Math.min(initialVectorBatchRows, maxBatchRows);
        boolean mayExpand = true;
        while (true) {
            long memoryUsed;
            try {
                arrowFormatWriter.write(columns, selected, startIndex, batchRows);
                memoryUsed = arrowFormatWriter.memoryUsed();
            } catch (OutOfMemoryException | OversizedAllocationException e) {
                clearArrowWriterForRetry();
                if (batchRows == 1) {
                    throw e;
                }
                batchRows = Math.max(1, batchRows / 2);
                mayExpand = false;
                continue;
            }

            if (memoryUsed > writeBatchMemory && batchRows > 1) {
                clearArrowWriterForRetry();
                batchRows = reducedBatchRows(batchRows, memoryUsed, writeBatchMemory);
                mayExpand = false;
                continue;
            }

            if (mayExpand) {
                int expandedBatchRows =
                        Math.min(
                                maxBatchRows,
                                rowsWithinMemory(batchRows, memoryUsed, writeBatchMemory));
                if (expandedBatchRows > batchRows) {
                    arrowFormatWriter.reset();
                    batchRows = expandedBatchRows;
                    continue;
                }
            }
            return batchRows;
        }
    }

    private void clearArrowWriterForRetry() {
        arrowFormatWriter.reset();
        arrowFormatWriter.getVectorSchemaRoot().clear();
    }

    private static int initialVectorBatchRows(
            RowType rowType, int writeBatchSize, long writeBatchMemory) {
        if (rowType.getFields().stream().anyMatch(field -> !isFixedWidth(field.type()))) {
            return Math.min(writeBatchSize, MEMORY_CHECK_INTERVAL);
        }

        // Include one byte per field as a conservative allowance for validity buffers.
        long estimatedBytesPerRow = (long) rowType.defaultSize() + rowType.getFieldCount();
        return Math.min(
                writeBatchSize,
                rowsWithinMemory(1, Math.max(1, estimatedBytesPerRow), writeBatchMemory));
    }

    private static boolean isFixedWidth(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    private static int reducedBatchRows(int batchRows, long memoryUsed, long writeBatchMemory) {
        return Math.min(batchRows - 1, rowsWithinMemory(batchRows, memoryUsed, writeBatchMemory));
    }

    private static int rowsWithinMemory(int batchRows, long memoryUsed, long writeBatchMemory) {
        if (memoryUsed <= 0) {
            return Integer.MAX_VALUE;
        }
        double estimatedRows = (double) writeBatchMemory * batchRows / memoryUsed;
        return Math.max(1, (int) Math.min(Integer.MAX_VALUE, estimatedRows));
    }

    private void writeRows(BundleRecords bundleRecords) {
        for (InternalRow row : bundleRecords) {
            addElement(row);
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        if (!suggestedCheck) {
            return false;
        }
        return nativeWriter.estimatedFileSize() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        Throwable throwable = null;

        if (!failed) {
            try {
                flush();
            } catch (Throwable t) {
                throwable = t;
            }
        }

        try {
            nativeWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            collectMetadata();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            arrowFormatWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        if (throwable != null) {
            rethrow(throwable);
        }
    }

    @Nullable
    @Override
    public Object writerMetadata() {
        return metadata;
    }

    private void collectMetadata() {
        int numRowGroups = nativeWriter.numRowGroups();
        List<Map<String, ColumnStatistics>> allStats = new ArrayList<>(numRowGroups);
        for (int i = 0; i < numRowGroups; i++) {
            allStats.add(nativeWriter.getRowGroupStatistics(i));
        }
        this.metadata = new MosaicWriterMetadata(numRowGroups, allStats, statsColumnNames);
    }

    private void writeDirectArrow(VectorSchemaRoot root, long rowCount) {
        flush();
        try {
            nativeWriter.write(root);
        } catch (RuntimeException | Error e) {
            failed = true;
            throw e;
        }
        directArrowRows += rowCount;
    }

    private static boolean hasSingleInputAllocatorRoot(VectorSchemaRoot root) {
        return !root.getFieldVectors().isEmpty()
                && ArrowUtils.hasSameRootAllocator(root, root.getVector(0).getAllocator());
    }

    long directArrowRows() {
        return directArrowRows;
    }

    long mosaicBundleFallbackRows() {
        return mosaicBundleFallbackRows;
    }

    long genericBundleRows() {
        return genericBundleRows;
    }

    private void flush() {
        if (arrowFormatWriter.empty()) {
            return;
        }
        arrowFormatWriter.flush();
        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        try {
            nativeWriter.write(vsr);
        } catch (RuntimeException | Error e) {
            failed = true;
            throw e;
        } finally {
            arrowFormatWriter.reset();
        }
    }

    private void checkNotFailed() {
        if (failed) {
            throw new IllegalStateException("Mosaic writer has failed");
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
            @Nullable MosaicWriter nativeWriter,
            @Nullable ArrowFormatWriter arrowFormatWriter,
            BufferAllocator allocator) {
        try {
            if (nativeWriter != null) {
                nativeWriter.close();
            }
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            if (arrowFormatWriter != null) {
                arrowFormatWriter.close();
            }
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }
    }

    @FunctionalInterface
    interface NativeWriterFactory {

        MosaicWriter create(
                OutputStream outputStream,
                Schema arrowSchema,
                WriterOptions options,
                BufferAllocator allocator);
    }
}

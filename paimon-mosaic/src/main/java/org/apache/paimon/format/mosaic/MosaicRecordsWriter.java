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
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.mosaic.WriterOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Mosaic records writer. */
public class MosaicRecordsWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MosaicRecordsWriter.class);

    private final ArrowFormatWriter arrowFormatWriter;
    private final MosaicWriter nativeWriter;
    private final BufferAllocator allocator;
    private final List<String> statsColumnNames;
    private final RowType rowType;
    private final Schema arrowSchema;
    @Nullable private Object verifiedDirectSchemaIdentity;
    private long directArrowRows;
    private long projectedArrowFallbackRows;
    private long genericBundleRows;
    private long directSchemaValidations;
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
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Failed to write row to Mosaic file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) {
        if (bundleRecords instanceof ArrowBundleRecords) {
            ArrowBundleRecords arrowBundle = (ArrowBundleRecords) bundleRecords;
            VectorSchemaRoot root = arrowBundle.getVectorSchemaRoot();
            RowType bundleRowType = arrowBundle.getRowType();
            Object schemaIdentity =
                    arrowBundle instanceof MosaicArrowBundleRecords
                            ? ((MosaicArrowBundleRecords) arrowBundle).schemaIdentity()
                            : null;
            boolean directCompatible =
                    schemaIdentity != null && schemaIdentity == verifiedDirectSchemaIdentity;
            if (!directCompatible) {
                directSchemaValidations++;
                directCompatible =
                        MosaicArrowSchemaCompatibility.matchesRowType(rowType, bundleRowType)
                                && MosaicArrowSchemaCompatibility.matchesWriter(
                                        arrowSchema, root.getSchema());
                if (directCompatible && schemaIdentity != null) {
                    verifiedDirectSchemaIdentity = schemaIdentity;
                }
            }
            if (directCompatible) {
                flush();
                nativeWriter.write(root);
                directArrowRows += arrowBundle.rowCount();
                return;
            }
            if (MosaicArrowSchemaCompatibility.matchesRowTypeByName(rowType, bundleRowType)
                    && root.getSchema().getFields().size() == rowType.getFieldCount()
                    && MosaicArrowSchemaCompatibility.matchesProjection(
                            rowType, root.getSchema())) {
                for (InternalRow row : new ArrowBatchReader(rowType, true).readBatch(root)) {
                    addElement(row);
                }
                projectedArrowFallbackRows += arrowBundle.rowCount();
                return;
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Arrow bundle schema is incompatible with Mosaic writer schema. "
                                    + "Expected row type: %s, actual row type: %s, "
                                    + "Expected fields: %s, actual fields: %s.",
                            rowType,
                            bundleRowType,
                            arrowSchema.getFields(),
                            root.getSchema().getFields()));
        }

        genericBundleRows += bundleRecords.rowCount();
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

        try {
            flush();
        } catch (Throwable t) {
            throwable = t;
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

        if (directArrowRows > 0 || projectedArrowFallbackRows > 0 || genericBundleRows > 0) {
            LOG.info(
                    "Mosaic bundle write paths: directArrowRows={}, "
                            + "projectedArrowFallbackRows={}, genericBundleRows={}, "
                            + "directSchemaValidations={}",
                    directArrowRows,
                    projectedArrowFallbackRows,
                    genericBundleRows,
                    directSchemaValidations);
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

    private void flush() {
        if (arrowFormatWriter.empty()) {
            return;
        }
        arrowFormatWriter.flush();
        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        nativeWriter.write(vsr);
        arrowFormatWriter.reset();
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

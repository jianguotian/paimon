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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.OversizedAllocationException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Synchronous write view owning only its bounded NULL-column padding. */
final class MosaicArrowBatchView implements AutoCloseable {

    private final List<FieldVector> owned = new ArrayList<>();
    @Nullable private BufferAllocator paddingAllocator;
    private VectorSchemaRoot root;
    private boolean closed;

    private MosaicArrowBatchView() {}

    static boolean canProject(RowType type, Schema source) {
        Map<String, Field> fields = new HashMap<>();
        for (Field field : source.getFields()) {
            if (fields.put(field.getName(), field) != null) {
                return false;
            }
        }
        Set<String> names = new HashSet<>();
        for (DataField data : type.getFields()) {
            Field expected = ArrowUtils.toArrowField(data.name(), data.id(), data.type(), 0);
            if (!names.add(expected.getName())
                    || !expected.getChildren().isEmpty()
                    || expected.getDictionary() != null) {
                return false;
            }
            Field actual = fields.get(expected.getName());
            if (actual == null) {
                if (!expected.isNullable()) {
                    return false;
                }
            } else if (!expected.getType().equals(actual.getType())
                    || !actual.getChildren().isEmpty()
                    || actual.getDictionary() != null
                    || (!expected.isNullable() && actual.isNullable())
                    || (!actual.getMetadata().isEmpty()
                            && !Objects.equals(expected.getMetadata(), actual.getMetadata()))) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    static MosaicArrowBatchView tryCreate(
            RowType type, VectorSchemaRoot source, long maxPaddingBytes) {
        if (source.getFieldVectors().isEmpty()
                || !canProject(type, source.getSchema())
                || !ArrowUtils.hasSameRootAllocator(source, source.getVector(0).getAllocator())) {
            return null;
        }
        MosaicArrowBatchView view = new MosaicArrowBatchView();
        try {
            Map<String, FieldVector> sourceVectors = new HashMap<>();
            for (FieldVector vector : source.getFieldVectors()) {
                sourceVectors.put(vector.getName(), vector);
            }
            List<Field> fields = new ArrayList<>();
            List<FieldVector> vectors = new ArrayList<>();
            for (DataField data : type.getFields()) {
                Field expected = ArrowUtils.toArrowField(data.name(), data.id(), data.type(), 0);
                FieldVector vector = sourceVectors.get(data.name());
                if (vector == null) {
                    if (view.paddingAllocator == null) {
                        view.paddingAllocator =
                                source.getVector(0)
                                        .getAllocator()
                                        .newChildAllocator(
                                                "mosaic-arrow-padding",
                                                0,
                                                Math.max(0, maxPaddingBytes));
                    }
                    vector = expected.createVector(view.paddingAllocator);
                    view.owned.add(vector);
                    vector.setInitialCapacity(source.getRowCount());
                    vector.allocateNew();
                    vector.setValueCount(source.getRowCount());
                }
                fields.add(expected);
                vectors.add(vector);
            }
            view.root = new VectorSchemaRoot(fields, vectors, source.getRowCount());
            return view;
        } catch (OutOfMemoryException | OversizedAllocationException allocationFailure) {
            // No native write has started: release padding and use the ordinary writer path.
            view.close();
            return null;
        } catch (RuntimeException | Error failure) {
            try {
                view.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    VectorSchemaRoot root() {
        return root;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Throwable failure = null;
        for (FieldVector vector : owned) {
            try {
                vector.close();
            } catch (Throwable t) {
                if (failure == null) {
                    failure = t;
                } else {
                    failure.addSuppressed(t);
                }
            }
        }
        try {
            if (paddingAllocator != null) {
                paddingAllocator.close();
            }
        } catch (Throwable t) {
            if (failure == null) {
                failure = t;
            } else {
                failure.addSuppressed(t);
            }
        }
        // Do not close root: its existing vectors are still owned by the reader.
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure != null) {
            throw (RuntimeException) failure;
        }
    }
}

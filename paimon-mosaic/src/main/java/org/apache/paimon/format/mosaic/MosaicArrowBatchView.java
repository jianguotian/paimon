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
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ExceptionUtils.rethrow;
import static org.apache.paimon.utils.IOUtils.closeAll;

/** Synchronous write view owning only its bounded NULL-column padding. */
final class MosaicArrowBatchView implements AutoCloseable {

    private final List<AutoCloseable> owned = new ArrayList<>();
    @Nullable private BufferAllocator paddingAllocator;
    private VectorSchemaRoot root;
    private boolean closed;

    private MosaicArrowBatchView() {}

    private static List<Field> fields(RowType type) {
        return type.getFields().stream()
                .map(f -> ArrowUtils.toArrowField(f.name(), f.id(), f.type(), 0))
                .collect(Collectors.toList());
    }

    static boolean matches(RowType expected, Schema actual) {
        return matches(fields(expected), actual.getFields());
    }

    static boolean matches(List<Field> expected, List<Field> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        Set<String> names = new HashSet<>();
        for (int i = 0; i < expected.size(); i++) {
            Field left = expected.get(i);
            Field right = actual.get(i);
            if (!names.add(right.getName())
                    || !compatible(left, right)
                    || left.isNullable() != right.isNullable()
                    || !matches(left.getChildren(), right.getChildren())) {
                return false;
            }
        }
        return true;
    }

    private static boolean compatible(Field expected, Field actual) {
        // Mosaic may omit metadata; retained metadata (including field IDs) must agree.
        return expected.getName().equals(actual.getName())
                && expected.getType().equals(actual.getType())
                && Objects.equals(expected.getDictionary(), actual.getDictionary())
                && (actual.getMetadata().isEmpty()
                        || Objects.equals(expected.getMetadata(), actual.getMetadata()));
    }

    static boolean canProject(RowType type, Schema source) {
        return canProject(fields(type), source.getFields());
    }

    private static boolean canProject(List<Field> expected, List<Field> source) {
        Map<String, Field> fields = new HashMap<>();
        for (Field field : source) {
            if (fields.put(field.getName(), field) != null) {
                return false;
            }
        }
        Set<String> names = new HashSet<>();
        for (Field field : expected) {
            if (!names.add(field.getName())
                    || !field.getChildren().isEmpty()
                    || field.getDictionary() != null) {
                return false;
            }
            Field actual = fields.get(field.getName());
            if (actual == null) {
                if (!field.isNullable()) {
                    return false;
                }
            } else if (!compatible(field, actual)
                    || !actual.getChildren().isEmpty()
                    || (!field.isNullable() && actual.isNullable())) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    static MosaicArrowBatchView tryCreate(
            RowType type, ArrowBundleRecords bundle, long maxPaddingBytes) {
        VectorSchemaRoot source = bundle.getVectorSchemaRoot();
        if (!type.equals(bundle.getRowType())
                || source.getFieldVectors().isEmpty()
                || !ArrowUtils.hasSameRootAllocator(source, source.getVector(0).getAllocator())) {
            return null;
        }
        List<Field> expected = fields(type);
        MosaicArrowBatchView view = new MosaicArrowBatchView();
        if (bundle.hasIdentityMapping() && matches(expected, source.getSchema().getFields())) {
            view.root = source;
            return view;
        }
        // Projection below uses exact names, matching the Mosaic reader's row view.
        if (!bundle.isCaseSensitive() || !canProject(expected, source.getSchema().getFields())) {
            return null;
        }
        try {
            Map<String, FieldVector> sourceVectors = new HashMap<>();
            for (FieldVector vector : source.getFieldVectors()) {
                sourceVectors.put(vector.getName(), vector);
            }
            List<FieldVector> vectors = new ArrayList<>();
            for (Field field : expected) {
                FieldVector vector = sourceVectors.get(field.getName());
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
                    vector = field.createVector(view.paddingAllocator);
                    view.owned.add(vector);
                    vector.setInitialCapacity(source.getRowCount());
                    vector.allocateNew();
                    vector.setValueCount(source.getRowCount());
                }
                vectors.add(vector);
            }
            view.root = new VectorSchemaRoot(expected, vectors, source.getRowCount());
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
        // Release only padding, then its allocator. Existing vectors still belong to the reader.
        owned.add(paddingAllocator);
        try {
            closeAll(owned, Throwable.class);
        } catch (Exception e) {
            rethrow(e);
        }
    }
}

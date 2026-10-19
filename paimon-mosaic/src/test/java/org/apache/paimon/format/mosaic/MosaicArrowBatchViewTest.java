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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests bounded schema adaptation without copying existing vectors. */
class MosaicArrowBatchViewTest {
    private static final RowType SOURCE =
            RowType.builder().field("a", DataTypes.INT().notNull()).build();
    private static final RowType TARGET =
            new RowType(
                    Arrays.asList(
                            new DataField(1, "missing", DataTypes.STRING()),
                            new DataField(0, "a", DataTypes.INT()),
                            new DataField(2, "missingNumber", DataTypes.DOUBLE())));

    @Test
    void testReorderNullableWideningAndNullPaddingBorrowExistingBuffers() {
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot source = source(allocator)) {
            long before = allocator.getAllocatedMemory();
            MosaicArrowBatchView view = MosaicArrowBatchView.tryCreate(TARGET, source, 1024);
            assertThat(view).isNotNull();
            assertThat(view.root().getVector(1)).isSameAs(source.getVector(0));
            assertThat(view.root().getSchema().getFields().get(1).isNullable()).isTrue();
            assertThat(view.root().getVector(0).getNullCount()).isEqualTo(2);
            assertThat(view.root().getVector(2).getNullCount()).isEqualTo(2);
            assertThat(view.root().getRowCount()).isEqualTo(2);
            view.close();
            view.close();
            assertThat(allocator.getAllocatedMemory()).isEqualTo(before);
            assertThat(((IntVector) source.getVector(0)).get(1)).isEqualTo(42);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 32})
    void testPaddingBudgetFallsBackAndReleasesPartialAllocation(long budget) {
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot source = source(allocator)) {
            long before = allocator.getAllocatedMemory();
            assertThat(MosaicArrowBatchView.tryCreate(TARGET, source, budget)).isNull();
            assertThat(allocator.getAllocatedMemory()).isEqualTo(before);
            assertThat(((IntVector) source.getVector(0)).get(0)).isEqualTo(41);
        }
    }

    @Test
    void testRejectsTypeChangeMissingRequiredFieldAndNestedAdaptation() {
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot source = source(allocator)) {
            for (RowType type :
                    Arrays.asList(
                            RowType.builder().field("a", DataTypes.BIGINT()).build(),
                            RowType.builder().field("missing", DataTypes.INT().notNull()).build(),
                            RowType.builder()
                                    .field("missing", DataTypes.ARRAY(DataTypes.INT()))
                                    .build())) {
                assertThat(MosaicArrowBatchView.tryCreate(type, source, 1024)).isNull();
            }
        }
    }

    private static VectorSchemaRoot source(RootAllocator allocator) {
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(SOURCE, allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(2);
        vector.set(0, 41);
        vector.set(1, 42);
        root.setRowCount(2);
        return root;
    }
}

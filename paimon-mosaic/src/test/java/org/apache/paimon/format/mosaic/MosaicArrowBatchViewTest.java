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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
            MosaicArrowBatchView view =
                    MosaicArrowBatchView.tryCreate(
                            TARGET, new ArrowBundleRecords(source, TARGET, true), 1024);
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
            assertThat(
                            MosaicArrowBatchView.tryCreate(
                                    TARGET, new ArrowBundleRecords(source, TARGET, true), budget))
                    .isNull();
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
                assertThat(
                                MosaicArrowBatchView.tryCreate(
                                        type, new ArrowBundleRecords(source, type, true), 1024))
                        .isNull();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("layouts")
    void testExactLayout(List<Field> expected, List<Field> actual, boolean compatible) {
        assertThat(MosaicArrowBatchView.matches(expected, actual)).isEqualTo(compatible);
    }

    private static Stream<Arguments> layouts() {
        Field a = ArrowUtils.toArrowField("a", 0, DataTypes.INT(), 0);
        Field b = ArrowUtils.toArrowField("b", 1, DataTypes.INT(), 0);
        Field differentType = ArrowUtils.toArrowField("a", 0, DataTypes.BIGINT(), 0);
        List<Field> expected = asList(a, b);
        return Stream.of(
                arguments(expected, expected, true),
                arguments(
                        expected,
                        asList(new Field("a", FieldType.nullable(a.getType()), null), b),
                        true),
                arguments(expected, singletonList(a), false),
                arguments(expected, asList(new Field("renamed", a.getFieldType(), null), b), false),
                arguments(expected, asList(differentType, b), false),
                arguments(
                        expected,
                        asList(ArrowUtils.toArrowField("a", 0, DataTypes.INT().notNull(), 0), b),
                        false),
                arguments(
                        expected,
                        asList(
                                new Field(
                                        "a",
                                        new FieldType(
                                                true,
                                                a.getType(),
                                                new DictionaryEncoding(
                                                        1, false, new ArrowType.Int(32, true))),
                                        null),
                                b),
                        false),
                arguments(
                        expected,
                        asList(ArrowUtils.toArrowField("a", 7, DataTypes.INT(), 0), b),
                        false),
                arguments(expected, asList(b, a), false),
                arguments(
                        singletonList(
                                new Field(
                                        "row",
                                        FieldType.nullable(ArrowType.Struct.INSTANCE),
                                        singletonList(a))),
                        singletonList(
                                new Field(
                                        "row",
                                        FieldType.nullable(ArrowType.Struct.INSTANCE),
                                        singletonList(differentType))),
                        false),
                arguments(asList(a, a), asList(a, a), false));
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

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

package org.apache.paimon.arrow.reader;

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowBatchReader}. */
class ArrowBatchReaderTest {

    @Test
    void testVectorizedBatchesAreIndependent() {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot firstRoot = intRoot(rowType, allocator, 11);
                VectorSchemaRoot secondRoot = intRoot(rowType, allocator, 22)) {
            ArrowBatchReader reader = new ArrowBatchReader(rowType, true);

            VectorizedColumnBatch first = reader.readVectorizedBatch(firstRoot);
            VectorizedColumnBatch second = reader.readVectorizedBatch(secondRoot);

            assertThat(first).isNotSameAs(second);
            assertThat(first.columns).isNotSameAs(second.columns);
            assertThat(first.getInt(0, 0)).isEqualTo(11);
            assertThat(second.getInt(0, 0)).isEqualTo(22);
        }
    }

    private static VectorSchemaRoot intRoot(RowType rowType, RootAllocator allocator, int value) {
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(1);
        vector.setSafe(0, value);
        vector.setValueCount(1);
        root.setRowCount(1);
        return root;
    }
}

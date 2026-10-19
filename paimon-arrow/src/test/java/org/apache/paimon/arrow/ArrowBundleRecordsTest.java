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

package org.apache.paimon.arrow;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests repeatable Arrow row views used by file indexes. */
class ArrowBundleRecordsTest {
    @Test
    void testRepeatedIterationAndRootReuseBetweenWrites() {
        RowType type = RowType.builder().field("a", DataTypes.INT()).build();
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(type, allocator)) {
            IntVector values = (IntVector) root.getVector(0);
            values.allocateNew(2);
            values.set(0, 41);
            values.set(1, 42);
            root.setRowCount(1);
            ArrowBundleRecords bundle = new ArrowBundleRecords(root, type, true);
            assertThat(bundle.isRepeatable()).isTrue();
            assertThat(read(bundle)).containsExactly(41);
            // A caller may reuse a root between writes, but never during a write.
            root.setRowCount(2);
            assertThat(bundle.rowCount()).isEqualTo(2);
            assertThat(read(bundle)).containsExactly(41, 42);
            assertThat(read(bundle)).containsExactly(41, 42);
        }
    }

    private static List<Integer> read(ArrowBundleRecords bundle) {
        List<Integer> values = new ArrayList<>();
        for (InternalRow row : bundle) {
            values.add(row.getInt(0));
        }
        return values;
    }
}

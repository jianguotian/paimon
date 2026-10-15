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
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Exact field-layout checks for exposing and writing original Mosaic Arrow batches. */
public final class MosaicArrowSchema {

    private MosaicArrowSchema() {}

    public static boolean matches(RowType expected, Schema actual) {
        return matchesFields(
                expected.getFields().stream()
                        .map(f -> ArrowUtils.toArrowField(f.name(), f.id(), f.type(), 0))
                        .collect(Collectors.toList()),
                actual.getFields());
    }

    public static boolean matches(Schema expected, Schema actual) {
        return matchesFields(expected.getFields(), actual.getFields());
    }

    private static boolean matchesFields(List<Field> expected, List<Field> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        Set<String> names = new HashSet<>();
        for (int i = 0; i < expected.size(); i++) {
            Field left = expected.get(i);
            Field right = actual.get(i);
            if (!names.add(right.getName())
                    || !left.getName().equals(right.getName())
                    || !left.getType().equals(right.getType())
                    || left.isNullable() != right.isNullable()
                    || !Objects.equals(
                            left.getFieldType().getDictionary(),
                            right.getFieldType().getDictionary())
                    || !matchesFields(left.getChildren(), right.getChildren())) {
                return false;
            }
            // Mosaic may omit field metadata. Any metadata it does retain must agree with the
            // expected schema, including field IDs. Never relax names, types or nullability.
            Map<String, String> metadata = right.getMetadata();
            if (metadata != null
                    && !metadata.isEmpty()
                    && !Objects.equals(left.getMetadata(), metadata)) {
                return false;
            }
        }
        return true;
    }
}

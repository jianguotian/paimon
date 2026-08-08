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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Compatibility checks for passing Arrow batches directly between Mosaic readers and writers. */
final class MosaicArrowSchemaCompatibility {

    private MosaicArrowSchemaCompatibility() {}

    static boolean matchesRowType(RowType expected, RowType actual) {
        if (expected.getFieldCount() != actual.getFieldCount()) {
            return false;
        }

        for (int i = 0; i < expected.getFieldCount(); i++) {
            if (!matchesDataField(expected.getFields().get(i), actual.getFields().get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether rows from {@code actual} can be encoded positionally with {@code expected}.
     *
     * <p>Top-level names and nullability may differ after schema evolution, but field ids, order,
     * and all other value type properties must still match.
     */
    static boolean matchesRowValues(RowType expected, RowType actual) {
        if (expected.getFieldCount() != actual.getFieldCount()) {
            return false;
        }

        for (int i = 0; i < expected.getFieldCount(); i++) {
            DataField expectedField = expected.getFields().get(i);
            DataField actualField = actual.getFields().get(i);
            if (expectedField.id() != actualField.id()
                    || !expectedField.type().equalsIgnoreNullable(actualField.type())) {
                return false;
            }
        }
        return true;
    }

    static boolean matchesBundle(RowType bundleRowType, Schema bundleSchema) {
        List<Field> rowFields =
                bundleRowType.getFields().stream()
                        .map(
                                field ->
                                        ArrowUtils.toArrowField(
                                                field.name(), field.id(), field.type(), 0))
                        .collect(Collectors.toList());
        return matchesFields(rowFields, bundleSchema.getFields(), true);
    }

    static boolean matchesProjection(RowType projectedRowType, Schema fileSchema) {
        List<Field> expectedFields =
                projectedRowType.getFields().stream()
                        .map(
                                field ->
                                        ArrowUtils.toArrowField(
                                                field.name(), field.id(), field.type(), 0))
                        .collect(Collectors.toList());

        Map<String, Field> fileFieldsByName = new HashMap<>();
        for (Field field : fileSchema.getFields()) {
            if (fileFieldsByName.put(field.getName(), field) != null) {
                return false;
            }
        }

        for (Field expectedField : expectedFields) {
            Field fileField = fileFieldsByName.get(expectedField.getName());
            if (fileField == null || !matchesField(expectedField, fileField, false)) {
                return false;
            }
        }
        return true;
    }

    static boolean matchesWriter(Schema expectedSchema, Schema actualSchema) {
        return matchesFields(expectedSchema.getFields(), actualSchema.getFields(), true);
    }

    private static boolean matchesFields(
            List<Field> expectedFields, List<Field> actualFields, boolean checkPresentMetadata) {
        if (expectedFields.size() != actualFields.size()) {
            return false;
        }

        for (int i = 0; i < expectedFields.size(); i++) {
            if (!matchesField(expectedFields.get(i), actualFields.get(i), checkPresentMetadata)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesDataField(DataField expected, DataField actual) {
        return actual != null
                && expected.id() == actual.id()
                && expected.name().equals(actual.name())
                && expected.type().equals(actual.type());
    }

    private static boolean matchesField(
            Field expected, Field actual, boolean checkPresentMetadata) {
        if (!expected.getName().equals(actual.getName())
                || !expected.getType().equals(actual.getType())
                || expected.isNullable() != actual.isNullable()
                || !Objects.equals(
                        expected.getFieldType().getDictionary(),
                        actual.getFieldType().getDictionary())) {
            return false;
        }

        // Mosaic 0.2.0 does not persist Arrow field metadata. If metadata is present, however,
        // require it to agree with the Paimon schema before passing buffers directly to the native
        // writer (notably PARQUET:field_id). A name-mapped fallback rewrites the rows into the
        // writer's own Arrow schema, so source metadata does not affect that path.
        Map<String, String> actualMetadata = actual.getMetadata();
        if (checkPresentMetadata
                && actualMetadata != null
                && !actualMetadata.isEmpty()
                && !Objects.equals(expected.getMetadata(), actualMetadata)) {
            return false;
        }

        List<Field> expectedChildren = expected.getChildren();
        List<Field> actualChildren = actual.getChildren();
        if (expectedChildren.size() != actualChildren.size()) {
            return false;
        }
        for (int i = 0; i < expectedChildren.size(); i++) {
            if (!matchesField(
                    expectedChildren.get(i), actualChildren.get(i), checkPresentMetadata)) {
                return false;
            }
        }
        return true;
    }
}

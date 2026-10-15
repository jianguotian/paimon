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
import org.apache.paimon.types.DataTypes;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Covers the field properties required to pass original buffers without conversion. */
class MosaicArrowSchemaTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "exact",
                "missing-metadata",
                "count",
                "name",
                "type",
                "nullable",
                "dictionary",
                "field-id",
                "order",
                "nested",
                "duplicate"
            })
    void testExactLayoutAndOptionalMetadata(String change) {
        Field a = ArrowUtils.toArrowField("a", 0, DataTypes.INT(), 0);
        Field b = ArrowUtils.toArrowField("b", 1, DataTypes.INT(), 0);
        Schema expected = new Schema(Arrays.asList(a, b));
        Field changed = a;
        switch (change) {
            case "missing-metadata":
                changed = new Field("a", FieldType.nullable(a.getType()), a.getChildren());
                break;
            case "name":
                changed = new Field("renamed", a.getFieldType(), a.getChildren());
                break;
            case "type":
                changed = ArrowUtils.toArrowField("a", 0, DataTypes.BIGINT(), 0);
                break;
            case "nullable":
                changed = ArrowUtils.toArrowField("a", 0, DataTypes.INT().notNull(), 0);
                break;
            case "dictionary":
                changed =
                        new Field(
                                "a",
                                new FieldType(
                                        true,
                                        a.getType(),
                                        new DictionaryEncoding(
                                                1, false, new ArrowType.Int(32, true))),
                                a.getChildren());
                break;
            case "field-id":
                changed = ArrowUtils.toArrowField("a", 7, DataTypes.INT(), 0);
                break;
            case "nested":
                expected =
                        new Schema(
                                Collections.singletonList(
                                        new Field(
                                                "row",
                                                FieldType.nullable(ArrowType.Struct.INSTANCE),
                                                Collections.singletonList(a))));
                changed =
                        new Field(
                                "row",
                                FieldType.nullable(ArrowType.Struct.INSTANCE),
                                Collections.singletonList(
                                        ArrowUtils.toArrowField("a", 0, DataTypes.BIGINT(), 0)));
                break;
            default:
                break;
        }
        Schema actual = new Schema(Arrays.asList(changed, b));
        if (change.equals("count") || change.equals("nested")) {
            actual = new Schema(Collections.singletonList(changed));
        } else if (change.equals("order")) {
            actual = new Schema(Arrays.asList(b, a));
        } else if (change.equals("duplicate")) {
            // Reject ambiguous names even if both schemas contain them.
            expected = new Schema(Arrays.asList(a, a));
            actual = expected;
        }
        assertThat(MosaicArrowSchema.matches(expected, actual))
                .isEqualTo(change.equals("exact") || change.equals("missing-metadata"));
    }
}

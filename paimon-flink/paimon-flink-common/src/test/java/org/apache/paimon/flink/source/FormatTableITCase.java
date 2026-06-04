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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.flink.RESTCatalogITCaseBase;
import org.apache.paimon.rest.RESTToken;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for format table. */
public class FormatTableITCase extends RESTCatalogITCaseBase {

    @Test
    public void testDiffFormat() {
        String bigDecimalStr = "10.001";
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal(bigDecimalStr), 8, 3);
        for (String format : new String[] {"parquet", "csv", "json"}) {
            String tableName = "format_table_parquet_" + format.toLowerCase();
            Identifier identifier = Identifier.create("default", tableName);
            sql(
                    "CREATE TABLE %s (a DECIMAL(8, 3), b INT, c INT) WITH ('file.format'='%s', 'type'='format-table')",
                    tableName, format);
            RESTToken expiredDataToken =
                    new RESTToken(
                            ImmutableMap.of(
                                    "akId",
                                    "akId-expire",
                                    "akSecret",
                                    UUID.randomUUID().toString()),
                            System.currentTimeMillis() + 1000_000);
            restCatalogServer.setDataToken(identifier, expiredDataToken);
            sql("INSERT INTO %s VALUES (%s, 1, 1), (%s, 2, 2)", tableName, decimal, decimal);
            assertThat(sql("SELECT a, b FROM %s", tableName))
                    .containsExactlyInAnyOrder(
                            Row.of(new BigDecimal(bigDecimalStr), 1),
                            Row.of(new BigDecimal(bigDecimalStr), 2));
            sql("Drop TABLE %s", tableName);
        }
    }

    @Test
    public void testPartitionedTableInsertOverwrite() {

        String ptTableName = "format_table_overwrite";
        Identifier ptIdentifier = Identifier.create("default", ptTableName);
        sql(
                "CREATE TABLE %s (a DECIMAL(8, 3), b INT, c INT) PARTITIONED BY (c) WITH ('file.format'='parquet', 'type'='format-table')",
                ptTableName);
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 1000_000);
        restCatalogServer.setDataToken(ptIdentifier, expiredDataToken);

        String ptBigDecimalStr1 = "10.001";
        String ptBigDecimalStr2 = "12.345";
        Decimal ptDecimal1 = Decimal.fromBigDecimal(new BigDecimal(ptBigDecimalStr1), 8, 3);
        Decimal ptDecimal2 = Decimal.fromBigDecimal(new BigDecimal(ptBigDecimalStr2), 8, 3);

        sql(
                "INSERT INTO %s PARTITION (c = 1) VALUES (%s, 10), (%s, 20)",
                ptTableName, ptDecimal1, ptDecimal1);
        sql("INSERT INTO %s PARTITION (c = 2) VALUES (%s, 30)", ptTableName, ptDecimal1);

        assertThat(sql("SELECT a, b, c FROM %s", ptTableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(ptBigDecimalStr1), 10, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr1), 20, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr1), 30, 2));

        sql(
                "INSERT OVERWRITE %s PARTITION (c = 1) VALUES (%s, 100), (%s, 200)",
                ptTableName, ptDecimal2, ptDecimal2);

        assertThat(sql("SELECT a, b, c FROM %s", ptTableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(ptBigDecimalStr2), 100, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr2), 200, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr1), 30, 2));

        sql(
                "INSERT OVERWRITE %s VALUES (%s, 100, 1), (%s, 200, 2)",
                ptTableName, ptDecimal1, ptDecimal2);

        assertThat(sql("SELECT a, b, c FROM %s", ptTableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(ptBigDecimalStr1), 100, 1),
                        Row.of(new BigDecimal(ptBigDecimalStr2), 200, 2));

        sql("Drop TABLE %s", ptTableName);
    }

    @Test
    public void testUnPartitionedTableInsertOverwrite() {
        String tableName = "format_table_overwrite_test";
        String bigDecimalStr1 = "10.001";
        String bigDecimalStr2 = "12.345";
        Decimal decimal1 = Decimal.fromBigDecimal(new BigDecimal(bigDecimalStr1), 8, 3);
        Decimal decimal2 = Decimal.fromBigDecimal(new BigDecimal(bigDecimalStr2), 8, 3);

        Identifier identifier = Identifier.create("default", tableName);
        sql(
                "CREATE TABLE %s (a DECIMAL(8, 3), b INT, c INT) WITH ('file.format'='parquet', 'type'='format-table')",
                tableName);
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 1000_000);
        restCatalogServer.setDataToken(identifier, expiredDataToken);

        sql("INSERT INTO %s VALUES (%s, 1, 1), (%s, 2, 2)", tableName, decimal1, decimal1);
        assertThat(sql("SELECT a, b FROM %s", tableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(bigDecimalStr1), 1),
                        Row.of(new BigDecimal(bigDecimalStr1), 2));

        sql("INSERT OVERWRITE %s VALUES (%s, 3, 3), (%s, 4, 4)", tableName, decimal2, decimal2);
        assertThat(sql("SELECT a, b FROM %s", tableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(bigDecimalStr2), 3),
                        Row.of(new BigDecimal(bigDecimalStr2), 4));

        sql("Drop TABLE %s", tableName);
    }

    @Test
    public void testPartitionPathHiveStyleStillDelegatesToFilesystem() {
        // Default (partition-path-only-value not set / false) must still keep the legacy
        // delegation to Flink's filesystem connector with Hive-style `key=value` partitions.
        String tableName = "format_table_hive_partition_default";
        sql(
                "CREATE TABLE %s (a INT, b STRING, dt STRING) "
                        + "PARTITIONED BY (dt) "
                        + "WITH ('file.format'='csv', 'type'='format-table')",
                tableName);

        sql("INSERT INTO %s PARTITION (dt='2026-06-04') VALUES (1, 'x'), (2, 'y')", tableName);
        sql("INSERT INTO %s PARTITION (dt='2026-06-05') VALUES (3, 'z')", tableName);

        assertThat(sql("SELECT a, b, dt FROM %s", tableName))
                .containsExactlyInAnyOrder(
                        Row.of(1, "x", "2026-06-04"),
                        Row.of(2, "y", "2026-06-04"),
                        Row.of(3, "z", "2026-06-05"));

        sql("Drop TABLE %s", tableName);
    }

    @Test
    public void testPartitionPathOnlyValue() {
        // SELECT used to return empty because FormatCatalogTable delegated to Flink's filesystem
        // connector (connector=filesystem), whose partition discovery only matches Hive-style
        // `key=value` directories. With format-table.partition-path-only-value=true the layout is
        // bare `value/...`, so the filesystem connector silently produced 0 splits. We now route
        // through paimon's own scan path (FormatReadBuilder + FormatTableScan) which honors the
        // option.
        String tableName = "format_table_partition_only_value";
        sql(
                "CREATE TABLE %s (a INT, b STRING, dt STRING, hh STRING) "
                        + "PARTITIONED BY (dt, hh) "
                        + "WITH ("
                        + "'file.format'='csv', "
                        + "'type'='format-table', "
                        + "'format-table.partition-path-only-value'='true'"
                        + ")",
                tableName);

        sql(
                "INSERT INTO %s PARTITION (dt='2026-06-04', hh='17') VALUES (1, 'x'), (2, 'y')",
                tableName);
        sql("INSERT INTO %s PARTITION (dt='2026-06-04', hh='18') VALUES (3, 'z')", tableName);

        assertThat(sql("SELECT a, b, dt, hh FROM %s", tableName))
                .containsExactlyInAnyOrder(
                        Row.of(1, "x", "2026-06-04", "17"),
                        Row.of(2, "y", "2026-06-04", "17"),
                        Row.of(3, "z", "2026-06-04", "18"));

        sql("Drop TABLE %s", tableName);
    }
}

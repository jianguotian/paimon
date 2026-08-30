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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.source.operator.MonitorSource;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.CONSUMER_EXPIRATION_TIME;
import static org.apache.paimon.CoreOptions.CONSUMER_ID;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_MAX_SNAPSHOT_COUNT;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FlinkSourceBuilder}. */
public class FlinkSourceBuilderTest {

    @TempDir Path tempDir;
    private Catalog catalog;

    @BeforeEach
    public void setUp() {
        try {
            initCatalog();
        } catch (Exception e) {
            throw new RuntimeException("Catalog initialization failed", e);
        }
    }

    private void initCatalog() throws Exception {
        if (catalog == null) {
            catalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
            catalog.createDatabase("default", false);
        }
    }

    private Table createTable(
            String tableName, boolean hasPrimaryKey, int bucketNum, boolean bucketAppendOrdered)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .option("bucket", bucketNum + "")
                        .option("bucket-append-ordered", String.valueOf(bucketAppendOrdered));

        if (hasPrimaryKey) {
            schemaBuilder.primaryKey("a");
        }

        if (bucketNum != -1) {
            schemaBuilder.option("bucket-key", "a");
        }

        Schema schema = schemaBuilder.build();
        Identifier identifier = Identifier.create("default", tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    @Test
    public void testUnawareBucket() throws Exception {
        // pk table && bucket-append-ordered is true
        Table table = createTable("t1", true, 2, true);
        FlinkSourceBuilder builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // pk table && bucket-append-ordered is false
        table = createTable("t2", true, 2, false);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // pk table && bucket num == -1 && bucket-append-ordered is false
        table = createTable("t3", true, -1, false);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // append table && bucket num != 1 && bucket-append-ordered is true
        table = createTable("t4", false, 2, true);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // append table && bucket num == -1
        table = createTable("t5", false, -1, true);
        builder = new FlinkSourceBuilder(table);
        assertTrue(builder.isUnordered());

        // append table && bucket-append-ordered is false
        table = createTable("t6", false, 2, false);
        builder = new FlinkSourceBuilder(table);
        assertTrue(builder.isUnordered());
    }

    @Test
    public void testBuildWrapsStaticSourceWithPaimonDataStreamSource() throws Exception {
        Table table = createTable("static_source", false, -1, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                new FlinkSourceBuilder(table).env(env).sourceBounded(true).build();

        assertThat(dataStream.getTransformation()).isInstanceOf(SourceTransformation.class);
        SourceTransformation<?, ?, ?> transformation =
                (SourceTransformation<?, ?, ?>) dataStream.getTransformation();
        assertThat(transformation.getSource()).isInstanceOf(PaimonDataStreamSource.class);
    }

    @Test
    public void testBuildWrapsContinuousSourceWithPaimonDataStreamSource() throws Exception {
        Table table = createTable("continuous_source", false, -1, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                new FlinkSourceBuilder(table).env(env).sourceBounded(false).build();

        assertThat(dataStream.getTransformation()).isInstanceOf(SourceTransformation.class);
        SourceTransformation<?, ?, ?> transformation =
                (SourceTransformation<?, ?, ?>) dataStream.getTransformation();
        assertThat(transformation.getSource()).isInstanceOf(PaimonDataStreamSource.class);
    }

    @Test
    public void testPostponeMergeOnReadRejectsContinuousSource() throws Exception {
        Identifier identifier = Identifier.create("default", "postpone_merge_on_read");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .primaryKey("a")
                        .option("bucket", "-2")
                        .option("postpone.merge-on-read", "true")
                        .build(),
                false);
        Table table = catalog.getTable(identifier);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(
                        () -> new FlinkSourceBuilder(table).env(env).sourceBounded(false).build())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("only supported for batch reads");
    }

    @Test
    public void testMonitorSourceBuildSourceWrapsWithPaimonDataStreamSource() throws Exception {
        Table table = createTable("monitor_source", false, -1, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                MonitorSource.buildSource(
                        env,
                        "source",
                        InternalTypeInfo.of(toLogicalType(table.rowType())),
                        table.newReadBuilder(),
                        10,
                        false,
                        false,
                        false,
                        null,
                        true,
                        null,
                        table);

        assertThat(dataStream.getTransformation().getTransitivePredecessors())
                .filteredOn(Transformation.class::isInstance)
                .filteredOn(transformation -> transformation instanceof SourceTransformation)
                .anySatisfy(
                        transformation ->
                                assertThat(
                                                ((SourceTransformation<?, ?, ?>) transformation)
                                                        .getSource())
                                        .isInstanceOf(PaimonDataStreamSource.class));
    }

    @Test
    public void testExactlyOnceMonitorSourceUsesConfiguredMaxSnapshotCount() throws Exception {
        Identifier identifier = Identifier.create("default", "limited_monitor_source");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .primaryKey("a")
                        .option("bucket", "1")
                        .option(CONSUMER_ID.key(), "limited_consumer")
                        .option(CONSUMER_EXPIRATION_TIME.key(), "1 d")
                        .option(SCAN_MAX_SNAPSHOT_COUNT.key(), "1")
                        .build(),
                false);
        Table table = catalog.getTable(identifier);
        writeToTable(table, 1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                new FlinkSourceBuilder(table).env(env).sourceBounded(false).build();
        SourceTransformation<?, ?, ?> sourceTransformation =
                dataStream.getTransformation().getTransitivePredecessors().stream()
                        .filter(SourceTransformation.class::isInstance)
                        .map(SourceTransformation.class::cast)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        @SuppressWarnings("unchecked")
        SourceReader<Split, SimpleSourceSplit> reader =
                (SourceReader<Split, SimpleSourceSplit>)
                        sourceTransformation.getSource().createReader(null);
        TestingReaderOutput<Split> output = new TestingReaderOutput<>();

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(output.records).hasSize(1);
        assertThat(reader.isAvailable()).isNotDone();
    }

    private void writeToTable(Table table, int value) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(GenericRow.of(value));
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();
    }

    private static class TestingReaderOutput<T> implements ReaderOutput<T> {

        private final List<T> records = new ArrayList<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void collect(T record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}

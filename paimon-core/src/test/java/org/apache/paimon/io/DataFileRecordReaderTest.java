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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataFileRecordReader}. */
class DataFileRecordReaderTest {

    @Test
    void testPassesFinalOutputTypeToColumnarMapping() throws IOException {
        RowType outputRowType = RowType.builder().field("renamed", DataTypes.INT()).build();
        TestingColumnarRowIterator iterator = new TestingColumnarRowIterator();
        FileRecordReader<InternalRow> formatReader =
                new FileRecordReader<InternalRow>() {
                    private boolean returned;

                    @Nullable
                    @Override
                    public FileRecordIterator<InternalRow> readBatch() {
                        if (returned) {
                            return null;
                        }
                        returned = true;
                        return iterator;
                    }

                    @Override
                    public void close() {}
                };

        try (DataFileRecordReader reader =
                new DataFileRecordReader(
                        outputRowType,
                        formatReader,
                        false,
                        false,
                        null,
                        null,
                        null,
                        false,
                        null,
                        0L,
                        Collections.emptyMap(),
                        null,
                        new Path("file:/tmp/data-file-record-reader-test"))) {
            assertThat(reader.readBatch()).isSameAs(iterator);
            assertThat(iterator.outputRowType).isSameAs(outputRowType);
        }
    }

    private static class TestingColumnarRowIterator extends ColumnarRowIterator {

        private RowType outputRowType;

        private TestingColumnarRowIterator() {
            super(
                    new Path("file:/tmp/data-file-record-reader-test"),
                    new ColumnarRow(new VectorizedColumnBatch(new ColumnVector[0])),
                    null);
        }

        @Override
        public ColumnarRowIterator mapping(
                RowType outputRowType,
                @Nullable PartitionInfo partitionInfo,
                @Nullable int[] indexMapping) {
            this.outputRowType = outputRowType;
            return this;
        }
    }
}

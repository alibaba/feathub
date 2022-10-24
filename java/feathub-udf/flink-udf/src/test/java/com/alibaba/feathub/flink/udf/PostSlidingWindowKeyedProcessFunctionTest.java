/*
 * Copyright 2022 The Feathub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.udf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.feathub.flink.udf.PostSlidingWindowUtils.postSlidingWindow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PostSlidingWindowKeyedProcessFunction}. */
public class PostSlidingWindowKeyedProcessFunctionTest {
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;

    @BeforeEach
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    void testPostSlidingDefaultValueExpiredRow() {
        final DataStream<Row> data =
                env.fromElements(
                        Row.of(0, 1, Instant.ofEpochMilli(0)),
                        Row.of(1, 1, Instant.ofEpochMilli(100)),
                        Row.of(0, 2, Instant.ofEpochMilli(500)),
                        Row.of(1, 2, Instant.ofEpochMilli(600)),
                        Row.of(0, 3, Instant.ofEpochMilli(4000)),
                        Row.of(0, 4, Instant.ofEpochMilli(5000)));
        Table inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.INT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");
        tEnv.createTemporaryView("input_table", inputTable);
        Table table =
                tEnv.sqlQuery(
                        "SELECT id, SUM(val) AS val_sum, window_time AS ts FROM TABLE("
                                + "   HOP("
                                + "       DATA => TABLE input_table,"
                                + "       TIMECOL => DESCRIPTOR(ts),"
                                + "       SLIDE => INTERVAL '1' SECOND,"
                                + "       SIZE => INTERVAL '2' SECOND))"
                                + "GROUP BY id, window_start, window_end, window_time");
        final Row defaultRow = Row.withNames();
        defaultRow.setField("val_sum", 0);
        table = postSlidingWindow(tEnv, table, 1000, defaultRow, false, "ts", "id");

        List<Row> expected =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, 3, Instant.ofEpochMilli(999)),
                        Row.ofKind(RowKind.INSERT, 0, 3, Instant.ofEpochMilli(1999)),
                        Row.ofKind(RowKind.INSERT, 0, 0, Instant.ofEpochMilli(2999)),
                        Row.ofKind(RowKind.INSERT, 0, 3, Instant.ofEpochMilli(4999)),
                        Row.ofKind(RowKind.INSERT, 0, 7, Instant.ofEpochMilli(5999)),
                        Row.ofKind(RowKind.INSERT, 0, 4, Instant.ofEpochMilli(6999)),
                        Row.ofKind(RowKind.INSERT, 0, 0, Instant.ofEpochMilli(7999)),
                        Row.ofKind(RowKind.INSERT, 1, 3, Instant.ofEpochMilli(999)),
                        Row.ofKind(RowKind.INSERT, 1, 3, Instant.ofEpochMilli(1999)),
                        Row.ofKind(RowKind.INSERT, 1, 0, Instant.ofEpochMilli(2999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        sortResult(actual);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testPostSlidingWindowDefaultValueExpireRowAndSkipSameWindowOutput() {
        final DataStream<Row> data =
                env.fromElements(
                        Row.of(0, 1, Instant.ofEpochMilli(0)),
                        Row.of(1, 1, Instant.ofEpochMilli(100)),
                        Row.of(0, 2, Instant.ofEpochMilli(500)),
                        Row.of(1, 2, Instant.ofEpochMilli(600)),
                        Row.of(0, 3, Instant.ofEpochMilli(4000)),
                        Row.of(0, 4, Instant.ofEpochMilli(5000)));
        Table inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.INT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");
        tEnv.createTemporaryView("input_table", inputTable);
        Table table =
                tEnv.sqlQuery(
                        "SELECT id, SUM(val) AS val_sum, window_time AS ts FROM TABLE("
                                + "   HOP("
                                + "       DATA => TABLE input_table,"
                                + "       TIMECOL => DESCRIPTOR(ts),"
                                + "       SLIDE => INTERVAL '1' SECOND,"
                                + "       SIZE => INTERVAL '2' SECOND))"
                                + "GROUP BY id, window_start, window_end, window_time");
        final Row defaultRow = Row.withNames();
        defaultRow.setField("val_sum", 0);
        table = postSlidingWindow(tEnv, table, 1000, defaultRow, true, "ts", "id");

        List<Row> expected =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, 3, Instant.ofEpochMilli(999)),
                        Row.ofKind(RowKind.INSERT, 0, 0, Instant.ofEpochMilli(2999)),
                        Row.ofKind(RowKind.INSERT, 0, 3, Instant.ofEpochMilli(4999)),
                        Row.ofKind(RowKind.INSERT, 0, 7, Instant.ofEpochMilli(5999)),
                        Row.ofKind(RowKind.INSERT, 0, 4, Instant.ofEpochMilli(6999)),
                        Row.ofKind(RowKind.INSERT, 0, 0, Instant.ofEpochMilli(7999)),
                        Row.ofKind(RowKind.INSERT, 1, 3, Instant.ofEpochMilli(999)),
                        Row.ofKind(RowKind.INSERT, 1, 0, Instant.ofEpochMilli(2999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        sortResult(actual);
        assertThat(actual).isEqualTo(expected);
    }

    private void sortResult(List<Row> result) {
        result.sort(
                (o1, o2) -> {
                    int i = (Integer) o1.getFieldAs("id") - (Integer) o2.getFieldAs("id");
                    if (i == 0) {
                        i =
                                (int)
                                        (((Instant) o1.getFieldAs("ts")).toEpochMilli()
                                                - ((Instant) o2.getFieldAs("ts")).toEpochMilli());
                    }
                    return i;
                });
    }
}

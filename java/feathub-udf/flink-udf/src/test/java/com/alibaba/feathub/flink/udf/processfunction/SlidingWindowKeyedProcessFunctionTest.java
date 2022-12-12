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

package com.alibaba.feathub.flink.udf.processfunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;
import com.alibaba.feathub.flink.udf.SlidingWindowUtils;
import com.alibaba.feathub.flink.udf.ValueCountsAggFunc;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SlidingWindowKeyedProcessFunction}. */
public class SlidingWindowKeyedProcessFunctionTest {
    private StreamTableEnvironment tEnv;
    private Table inputTable;

    @BeforeEach
    void setUp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        final DataStream<Row> data =
                env.fromElements(
                        Row.of(0, 1L, Instant.ofEpochMilli(0)),
                        Row.of(1, 2L, Instant.ofEpochMilli(600)),
                        Row.of(1, 3L, Instant.ofEpochMilli(1100)),
                        Row.of(0, 4L, Instant.ofEpochMilli(5000)),
                        Row.of(0, 3L, Instant.ofEpochMilli(4000)),
                        Row.of(0, 5L, Instant.ofEpochMilli(6000)));
        inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");
    }

    @Test
    void testMultiSlidingWindowSizeProcessFunction() {
        tEnv.createTemporaryView("input_table", inputTable);

        Table table =
                tEnv.sqlQuery(
                        "SELECT * FROM TABLE("
                                + "   HOP("
                                + "       DATA => TABLE input_table,"
                                + "       TIMECOL => DESCRIPTOR(ts),"
                                + "       SLIDE => INTERVAL '1' SECOND,"
                                + "       SIZE => INTERVAL '1' SECOND))");

        table =
                table.groupBy($("id"), $("window_start"), $("window_end"), $("window_time"))
                        .select(
                                $("id"),
                                $("val").sum().as("val_sum"),
                                Expressions.row($("val").sum(), $("val").count()).as("val_avg"),
                                Expressions.call(ValueCountsAggFunc.class, $("val"))
                                        .as("val_value_counts"),
                                $("window_time"));

        table =
                SlidingWindowUtils.applySlidingWindowKeyedProcessFunction(
                        tEnv,
                        table,
                        Arrays.array("id"),
                        "window_time",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val_sum",
                                        DataTypes.BIGINT(),
                                        "val_sum_1",
                                        DataTypes.BIGINT(),
                                        1000L,
                                        "SUM")
                                .addField(
                                        "val_sum",
                                        DataTypes.BIGINT(),
                                        "val_sum_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "SUM")
                                .addField(
                                        "val_avg",
                                        table.getResolvedSchema()
                                                .getColumn("val_avg")
                                                .orElseThrow(RuntimeException::new)
                                                .getDataType(),
                                        "val_avg_1",
                                        DataTypes.FLOAT(),
                                        1000L,
                                        "AVG")
                                .addField(
                                        "val_avg",
                                        table.getResolvedSchema()
                                                .getColumn("val_avg")
                                                .orElseThrow(RuntimeException::new)
                                                .getDataType(),
                                        "val_avg_2",
                                        DataTypes.DOUBLE(),
                                        2000L,
                                        "AVG")
                                .addField(
                                        "val_value_counts",
                                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                        "val_value_counts_2",
                                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                        2000L,
                                        "VALUE_COUNTS")
                                .build());

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(
                                1,
                                2L,
                                2L,
                                2.0f,
                                2.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                1,
                                3L,
                                5L,
                                3.0f,
                                2.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(
                                1,
                                0L,
                                3L,
                                null,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(2999)),
                        Row.of(
                                0,
                                1L,
                                1L,
                                1.0f,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                0,
                                0L,
                                1L,
                                null,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(
                                0,
                                3L,
                                3L,
                                3.0f,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(4999)),
                        Row.of(
                                0,
                                4L,
                                7L,
                                4.0f,
                                3.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                        put(4L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(5999)),
                        Row.of(
                                0,
                                5L,
                                9L,
                                5.0f,
                                4.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(4L, 1L);
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(6999)),
                        Row.of(
                                0,
                                0L,
                                5L,
                                null,
                                5.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(7999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }
}

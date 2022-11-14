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
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.rowInterval;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ValueCountsAggFunc}. */
public class ValueCountsAggFuncTest {

    private StreamTableEnvironment tEnv;
    private Table inputTable;

    @BeforeEach
    void setUp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        final DataStream<Row> data =
                env.fromElements(
                        Row.of(0, "a", Instant.ofEpochMilli(1000)),
                        Row.of(0, "b", Instant.ofEpochMilli(3000)),
                        Row.of(0, "b", Instant.ofEpochMilli(2000)),
                        Row.of(0, "c", Instant.ofEpochMilli(4000)));

        inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.STRING())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");
    }

    @Test
    void testValueCountsOverWindow() {
        final Table table =
                inputTable
                        .window(
                                Over.partitionBy($("id"))
                                        .orderBy($("ts"))
                                        .preceding(rowInterval(2L))
                                        .as("w"))
                        .select(
                                $("id"),
                                call(ValueCountsAggFunc.class, $("val"))
                                        .over($("w"))
                                        .as("value_cnts"));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        List<Row> expected =
                Arrays.asList(
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("a", 1L);
                                    }
                                }),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("a", 1L);
                                        put("b", 1L);
                                    }
                                }),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("a", 1L);
                                        put("b", 2L);
                                    }
                                }),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("b", 2L);
                                        put("c", 1L);
                                    }
                                }));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testValueCountsSlidingWindow() {
        tEnv.createTemporaryView("input_table", inputTable);
        tEnv.createTemporaryFunction("VALUE_COUNTS", ValueCountsAggFunc.class);
        final Table table =
                tEnv.sqlQuery(
                        "SELECT id, VALUE_COUNTS(val) AS value_cnts, window_time AS ts FROM TABLE("
                                + "   HOP("
                                + "       DATA => TABLE input_table,"
                                + "       TIMECOL => DESCRIPTOR(ts),"
                                + "       SLIDE => INTERVAL '1' SECOND,"
                                + "       SIZE => INTERVAL '3' SECOND))"
                                + "GROUP BY id, window_start, window_end, window_time");

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        List<Row> expected =
                Arrays.asList(
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("a", 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("a", 1L);
                                        put("b", 1L);
                                    }
                                },
                                Instant.ofEpochMilli(2999)),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("a", 1L);
                                        put("b", 2L);
                                    }
                                },
                                Instant.ofEpochMilli(3999)),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("b", 2L);
                                        put("c", 1L);
                                    }
                                },
                                Instant.ofEpochMilli(4999)),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("b", 1L);
                                        put("c", 1L);
                                    }
                                },
                                Instant.ofEpochMilli(5999)),
                        Row.of(
                                0,
                                new HashMap<String, Long>() {
                                    {
                                        put("c", 1L);
                                    }
                                },
                                Instant.ofEpochMilli(6999)));
        assertThat(actual).isEqualTo(expected);
    }
}

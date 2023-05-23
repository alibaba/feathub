/*
 * Copyright 2022 The FeatHub Authors
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
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.rowInterval;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CollectListAggFunc}. */
public class CollectListAggFuncTest {

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
    void testCollectListOverWindow() {
        final Table table =
                inputTable
                        .window(
                                Over.partitionBy($("id"))
                                        .orderBy($("ts"))
                                        .preceding(rowInterval(2L))
                                        .as("w"))
                        .select(
                                $("id"),
                                call(CollectListAggFunc.class, $("val"))
                                        .over($("w"))
                                        .as("collect_lists"));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, new String[] {"a"}),
                        Row.of(0, new String[] {"a", "b"}),
                        Row.of(0, new String[] {"a", "b", "b"}),
                        Row.of(0, new String[] {"b", "b", "c"}));
        assertThat(actual).isEqualTo(expected);
    }
}

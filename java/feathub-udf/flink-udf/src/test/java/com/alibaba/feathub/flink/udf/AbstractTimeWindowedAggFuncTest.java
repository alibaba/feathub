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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.callSql;
import static org.apache.flink.table.api.Expressions.rowInterval;
import static org.assertj.core.api.Assertions.assertThat;

/** Base class for TimeWindowAggFunction tests. */
public abstract class AbstractTimeWindowedAggFuncTest {

    protected Table inputTable;

    @BeforeEach
    public void setUp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final DataStream<Row> data = getData(env);

        inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.INT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");
    }

    protected DataStreamSource<Row> getData(StreamExecutionEnvironment env) {
        return env.fromElements(
                Row.of(0, 1, Instant.ofEpochMilli(1000), ZoneId.systemDefault()),
                Row.of(0, 3, Instant.ofEpochMilli(3000), ZoneId.systemDefault()),
                Row.of(0, 2, Instant.ofEpochMilli(2000), ZoneId.systemDefault()),
                Row.of(0, 4, Instant.ofEpochMilli(4000), ZoneId.systemDefault()));
    }

    protected abstract Class<? extends AbstractTimeWindowedAggFunc<?, ?>> getAggFunc();

    protected void internalTest(DataType valueType, List<Row> expected) {
        final Table table =
                inputTable
                        .addOrReplaceColumns($("val").cast(valueType).as("val"))
                        .window(
                                Over.partitionBy($("id"))
                                        .orderBy($("ts"))
                                        .preceding(rowInterval(3L))
                                        .as("w"))
                        .select(
                                $("id"),
                                call(
                                                getAggFunc(),
                                                callSql("INTERVAL '1' SECONDS"),
                                                $("val"),
                                                $("ts"))
                                        .over($("w"))
                                        .as("value_cnts"));
        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).isEqualTo(expected);
    }
}

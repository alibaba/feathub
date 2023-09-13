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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncAdaptor;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithLimit;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithLimitWithoutRetract;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_RANGE;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for using {@link AggFuncAdaptor} in over window. */
public class OverWindowTest {

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

    @Test
    void testTimeRangedOverWindow() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0), Row.of(0, 1.5), Row.of(0, 2.0), Row.of(0, 2.5));
        applyOverWindow(
                DataTypes.INT(),
                expected,
                new AggFuncAdaptor<>(AggFuncUtils.getAggFunc("AVG", DataTypes.INT(), true)),
                lit(3).seconds());
    }

    @Test
    void testTimeRangedOverWindowWithLimit() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0), Row.of(0, 1.5), Row.of(0, 2.5), Row.of(0, 3.5));
        AggFunc<?, ?, ?> aggFunc = AggFuncUtils.getAggFunc("AVG", DataTypes.INT(), true);
        aggFunc = new AggFuncWithLimit<>(aggFunc, 2);
        applyOverWindow(DataTypes.INT(), expected, new AggFuncAdaptor<>(aggFunc), lit(3).seconds());
    }

    @Test
    void testUnboundedOverWindowWithLimit() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0), Row.of(0, 1.5), Row.of(0, 2.0), Row.of(0, 3.0));
        AggFunc<?, ?, ?> aggFunc = AggFuncUtils.getAggFunc("AVG", DataTypes.INT(), true);
        aggFunc = new AggFuncWithLimitWithoutRetract<>(aggFunc, 3);
        applyOverWindow(DataTypes.INT(), expected, new AggFuncAdaptor<>(aggFunc), UNBOUNDED_RANGE);
    }

    @Test
    void testUnboundedOverWindow() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0), Row.of(0, 1.5), Row.of(0, 2.0), Row.of(0, 2.5));
        AggFunc<?, ?, ?> aggFunc = AggFuncUtils.getAggFunc("AVG", DataTypes.INT(), true);
        applyOverWindow(DataTypes.INT(), expected, new AggFuncAdaptor<>(aggFunc), UNBOUNDED_RANGE);
    }

    protected DataStreamSource<Row> getData(StreamExecutionEnvironment env) {
        return env.fromElements(
                Row.of(0, 1, Instant.ofEpochMilli(1000), ZoneId.systemDefault()),
                Row.of(0, 3, Instant.ofEpochMilli(3000), ZoneId.systemDefault()),
                Row.of(0, 2, Instant.ofEpochMilli(2000), ZoneId.systemDefault()),
                Row.of(0, 4, Instant.ofEpochMilli(4000), ZoneId.systemDefault()));
    }

    protected void applyOverWindow(
            DataType valueType,
            List<Row> expected,
            AggFuncAdaptor<?, ?, ?> aggFunc,
            Expression rangeExpr) {
        final Table table =
                inputTable
                        .addOrReplaceColumns($("val").cast(valueType).as("val"))
                        .window(
                                Over.partitionBy($("id"))
                                        .orderBy($("ts"))
                                        .preceding(rangeExpr)
                                        .as("w"))
                        .select(
                                $("id"),
                                call(aggFunc, $("val"), $("ts")).over($("w")).as("value_cnts"));
        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i).getArity()).isEqualTo(expected.get(i).getArity());
            for (int j = 0; j < actual.get(i).getArity(); j++) {
                if (actual.get(i).getField(j).getClass().isArray()) {
                    assertThat((Object[]) actual.get(i).getField(j))
                            .containsExactly((Object[]) expected.get(i).getField(j));
                } else {
                    assertThat(actual.get(i).getField(j)).isEqualTo(expected.get(i).getField(j));
                }
            }
        }
    }
}

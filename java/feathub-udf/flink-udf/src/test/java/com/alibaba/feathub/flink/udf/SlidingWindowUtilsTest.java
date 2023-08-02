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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SlidingWindowUtils}. */
class SlidingWindowUtilsTest {

    @Test
    void testApplyGlobalWindow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final DataStream<Row> data =
                env.fromElements(
                        Row.of(0, 1L, Instant.ofEpochMilli(0)),
                        Row.of(1, 2L, Instant.ofEpochMilli(600)),
                        Row.of(1, 3L, Instant.ofEpochMilli(1100)),
                        Row.of(0, 3L, Instant.ofEpochMilli(4000)),
                        Row.of(0, 4L, Instant.ofEpochMilli(5000)),
                        Row.of(0, 5L, Instant.ofEpochMilli(6000)));
        Table inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");

        SlidingWindowDescriptor windowDescriptor =
                new SlidingWindowDescriptor(Duration.ZERO, Collections.singletonList("id"));

        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField(
                                "val_sum",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                0L,
                                null,
                                null,
                                "SUM")
                        .addField(
                                "val_count",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                0L,
                                null,
                                null,
                                "COUNT")
                        .build();

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            inputTable = inputTable.addOrReplaceColumns($("val").as(descriptor.fieldName));
        }

        final Table res =
                SlidingWindowUtils.applyGlobalWindow(
                        tEnv, inputTable, windowDescriptor, aggDescriptors, "ts");

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(1, 2L, 1L, Instant.ofEpochMilli(600)),
                        Row.of(1, 5L, 2L, Instant.ofEpochMilli(1100)),
                        Row.of(0, 1L, 1L, Instant.ofEpochMilli(0)),
                        Row.of(0, 4L, 2L, Instant.ofEpochMilli(4000)),
                        Row.of(0, 8L, 3L, Instant.ofEpochMilli(5000)),
                        Row.of(0, 13L, 4L, Instant.ofEpochMilli(6000)));

        List<Row> actual = CollectionUtil.iteratorToList(res.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testApplyGlobalWindowSkipSameWindowOutput() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final DataStream<Row> data =
                env.fromElements(
                        Row.of(0, 0L, Instant.ofEpochMilli(0)),
                        Row.of(0, 1L, Instant.ofEpochMilli(4000)),
                        Row.of(0, 0L, Instant.ofEpochMilli(5000)),
                        Row.of(0, 2L, Instant.ofEpochMilli(6000)));
        Table inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");

        SlidingWindowDescriptor windowDescriptor =
                new SlidingWindowDescriptor(Duration.ZERO, Collections.singletonList("id"));

        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField(
                                "val_sum",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                0L,
                                null,
                                null,
                                "SUM")
                        .build();

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            inputTable = inputTable.addOrReplaceColumns($("val").as(descriptor.fieldName));
        }

        final Table res =
                SlidingWindowUtils.applyGlobalWindow(
                        tEnv, inputTable, windowDescriptor, aggDescriptors, "ts");

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(0, 0L, Instant.ofEpochMilli(0)),
                        Row.of(0, 1L, Instant.ofEpochMilli(4000)),
                        Row.of(0, 3L, Instant.ofEpochMilli(6000)));

        List<Row> actual = CollectionUtil.iteratorToList(res.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }
}

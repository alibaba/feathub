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

package com.alibaba.feathub.flink.udf.processfunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;
import com.alibaba.feathub.flink.udf.SlidingWindowDescriptor;
import com.alibaba.feathub.flink.udf.SlidingWindowUtils;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private void verifyResult(
            AggregationFieldsDescriptor aggDescriptors, Row zeroValuedRow, List<Row> expected) {

        SlidingWindowDescriptor windowDescriptor =
                new SlidingWindowDescriptor(
                        Duration.ofSeconds(1), null, Collections.singletonList("id"), null);

        Table table = inputTable;
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            table = table.addOrReplaceColumns($("val").as(descriptor.fieldName));
        }

        DataStream<Row> stream =
                SlidingWindowUtils.applySlidingWindowPreAggregationProcess(
                        tEnv, table, windowDescriptor, aggDescriptors, "ts");

        Map<String, DataType> dataTypeMap =
                new HashMap<String, DataType>() {
                    {
                        put("id", DataTypes.INT());
                        put("val", DataTypes.BIGINT());
                        put("ts", DataTypes.TIMESTAMP_LTZ(3));
                    }
                };

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            dataTypeMap.put(descriptor.fieldName, descriptor.dataType);
        }

        table =
                SlidingWindowUtils.applySlidingWindowAggregationProcess(
                        tEnv,
                        stream,
                        dataTypeMap,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        aggDescriptors,
                        zeroValuedRow,
                        false);

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testMultiSlidingWindowSizeProcessFunction() {
        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField("val_sum_1", DataTypes.BIGINT(), DataTypes.BIGINT(), 1000L, "SUM")
                        .addField("val_sum_2", DataTypes.BIGINT(), DataTypes.BIGINT(), 2000L, "SUM")
                        .addField("val_avg_1", DataTypes.BIGINT(), DataTypes.FLOAT(), 1000L, "AVG")
                        .addField("val_avg_2", DataTypes.BIGINT(), DataTypes.DOUBLE(), 2000L, "AVG")
                        .addField(
                                "val_value_counts_2",
                                DataTypes.BIGINT(),
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                2000L,
                                "VALUE_COUNTS")
                        .build();

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

        verifyResult(aggDescriptors, null, expected);
    }

    @Test
    void testEnableEmptyWindowOutputDisableSameWindowOutput() {
        final Row zeroValuedRow = Row.withNames();
        zeroValuedRow.setField("val_sum_2", 0);
        zeroValuedRow.setField("val_avg_2", null);
        zeroValuedRow.setField("val_value_counts_2", null);

        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField("val_sum_2", DataTypes.BIGINT(), DataTypes.BIGINT(), 2000L, "SUM")
                        .addField("val_avg_2", DataTypes.BIGINT(), DataTypes.DOUBLE(), 2000L, "AVG")
                        .addField(
                                "val_value_counts_2",
                                DataTypes.BIGINT(),
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                2000L,
                                "VALUE_COUNTS")
                        .build();

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(
                                1,
                                2L,
                                2.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                1,
                                5L,
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
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(2999)),
                        Row.of(1, 0L, null, null, Instant.ofEpochMilli(3999)),
                        Row.of(
                                0,
                                1L,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                0,
                                1L,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(2999)),
                        Row.of(
                                0,
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(4999)),
                        Row.of(
                                0,
                                7L,
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
                                9L,
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
                                5L,
                                5.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(8999)));

        verifyResult(aggDescriptors, zeroValuedRow, expected);
    }

    @Test
    void testEnableEmptyWindowOutputAndSameWindowOutput() {
        final Row zeroValuedRow = Row.withNames();
        zeroValuedRow.setField("val_sum_2", 0);
        zeroValuedRow.setField("val_avg_2", null);
        zeroValuedRow.setField("val_value_counts_2", null);

        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField("val_sum_2", DataTypes.BIGINT(), DataTypes.BIGINT(), 2000L, "SUM")
                        .addField("val_avg_2", DataTypes.BIGINT(), DataTypes.DOUBLE(), 2000L, "AVG")
                        .addField(
                                "val_value_counts_2",
                                DataTypes.BIGINT(),
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                2000L,
                                "VALUE_COUNTS")
                        .build();

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(
                                1,
                                2L,
                                2.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                1,
                                5L,
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
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(2999)),
                        Row.of(1, 0L, null, null, Instant.ofEpochMilli(3999)),
                        Row.of(
                                0,
                                1L,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                0,
                                1L,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(2999)),
                        Row.of(
                                0,
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(4999)),
                        Row.of(
                                0,
                                7L,
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
                                9L,
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
                                5L,
                                5.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(8999)));

        verifyResult(aggDescriptors, zeroValuedRow, expected);
    }

    @Test
    void testMinMax() {
        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField("val_max_1", DataTypes.BIGINT(), DataTypes.BIGINT(), 1000L, "MAX")
                        .addField("val_max_2", DataTypes.BIGINT(), DataTypes.BIGINT(), 2000L, "MAX")
                        .addField("val_min_1", DataTypes.BIGINT(), DataTypes.BIGINT(), 1000L, "MIN")
                        .addField("val_min_2", DataTypes.BIGINT(), DataTypes.BIGINT(), 2000L, "MIN")
                        .build();

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(1, 2L, 2L, 2L, 2L, Instant.ofEpochMilli(999)),
                        Row.of(1, 3L, 3L, 3L, 2L, Instant.ofEpochMilli(1999)),
                        Row.of(1, null, 3L, null, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 1L, 1L, 1L, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, null, 1L, null, 1L, Instant.ofEpochMilli(1999)),
                        Row.of(0, 3L, 3L, 3L, 3L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, 4L, 4L, 3L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 5L, 5L, 5L, 4L, Instant.ofEpochMilli(6999)),
                        Row.of(0, null, 5L, null, 5L, Instant.ofEpochMilli(7999)));

        verifyResult(aggDescriptors, null, expected);
    }

    @Test
    void testFirstValue() {
        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField(
                                "val_first_value_1",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                1000L,
                                "FIRST_VALUE")
                        .addField(
                                "val_first_value_2",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                2000L,
                                "FIRST_VALUE")
                        .build();

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(1, 2L, 2L, Instant.ofEpochMilli(999)),
                        Row.of(1, 3L, 2L, Instant.ofEpochMilli(1999)),
                        Row.of(1, null, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 1L, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, null, 1L, Instant.ofEpochMilli(1999)),
                        Row.of(0, 3L, 3L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, 3L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 5L, 4L, Instant.ofEpochMilli(6999)),
                        Row.of(0, null, 5L, Instant.ofEpochMilli(7999)));

        verifyResult(aggDescriptors, null, expected);
    }

    @Test
    void testLastValue() {
        AggregationFieldsDescriptor aggDescriptors =
                AggregationFieldsDescriptor.builder()
                        .addField(
                                "val_last_value_1",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                1000L,
                                "LAST_VALUE")
                        .addField(
                                "val_last_value_2",
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                2000L,
                                "LAST_VALUE")
                        .build();

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(1, 2L, 2L, Instant.ofEpochMilli(999)),
                        Row.of(1, 3L, 3L, Instant.ofEpochMilli(1999)),
                        Row.of(1, null, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 1L, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, null, 1L, Instant.ofEpochMilli(1999)),
                        Row.of(0, 3L, 3L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, 4L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 5L, 5L, Instant.ofEpochMilli(6999)),
                        Row.of(0, null, 5L, Instant.ofEpochMilli(7999)));

        verifyResult(aggDescriptors, null, expected);
    }
}

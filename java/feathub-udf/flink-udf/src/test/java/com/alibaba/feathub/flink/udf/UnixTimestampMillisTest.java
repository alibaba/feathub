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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UnixTimestampMillis} and {@link FromUnixTimestampMillis}. */
public class UnixTimestampMillisTest {

    @Test
    void testFromToUnixTimestampMillis() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Row> expectedRow =
                Arrays.asList(
                        Row.of("2022-01-01 00:00:00.001"),
                        Row.of("2022-01-01 00:00:01.002"),
                        Row.of("2022-01-01 00:01:00.999"));
        Table table = tEnv.fromValues(expectedRow).as("ts");

        tEnv.createTemporaryFunction("UNIX_TIMESTAMP_MILLIS", UnixTimestampMillis.class);
        tEnv.createTemporaryFunction("FROM_UNIXTIME_MILLIS", FromUnixTimestampMillis.class);

        table =
                table.select(
                        Expressions.callSql(
                                        String.format(
                                                "UNIX_TIMESTAMP_MILLIS(`ts`, '%s')",
                                                tEnv.getConfig().getLocalTimeZone()))
                                .as("epoch_millis"));

        final List<Row> epochMillis = CollectionUtil.iteratorToList(table.execute().collect());

        table = tEnv.fromValues(epochMillis).as("epoch_millis");
        table =
                table.select(
                        Expressions.callSql(
                                        String.format(
                                                "CAST(FROM_UNIXTIME_MILLIS(`epoch_millis`, '%s') AS STRING)",
                                                tEnv.getConfig().getLocalTimeZone()))
                                .as("ts"));

        final List<Row> timestamps = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(timestamps).isEqualTo(expectedRow);
    }
}

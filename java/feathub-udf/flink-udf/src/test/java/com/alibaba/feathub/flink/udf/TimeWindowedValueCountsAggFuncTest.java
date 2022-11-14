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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Test for {@link TimeWindowedValueCountsAggFunc}. */
public class TimeWindowedValueCountsAggFuncTest extends AbstractTimeWindowedAggFuncTest {

    @Override
    protected DataStreamSource<Row> getData(StreamExecutionEnvironment env) {
        return env.fromElements(
                Row.of(
                        0,
                        1,
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(1000), ZoneId.systemDefault())),
                Row.of(
                        0,
                        2,
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(3000), ZoneId.systemDefault())),
                Row.of(
                        0,
                        2,
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(2000), ZoneId.systemDefault())),
                Row.of(
                        0,
                        3,
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(4000), ZoneId.systemDefault())));
    }

    @Test
    void testTimeWindowedValueCountsAggFuncInt() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(
                                0,
                                new HashMap<Integer, Long>() {
                                    {
                                        put(1, 1L);
                                    }
                                }),
                        Row.of(
                                0,
                                new HashMap<Integer, Long>() {
                                    {
                                        put(1, 1L);
                                        put(2, 1L);
                                    }
                                }),
                        Row.of(
                                0,
                                new HashMap<Integer, Long>() {
                                    {
                                        put(2, 2L);
                                    }
                                }),
                        Row.of(
                                0,
                                new HashMap<Integer, Long>() {
                                    {
                                        put(2, 1L);
                                        put(3, 1L);
                                    }
                                }));
        internalTest(DataTypes.INT(), expected);
    }

    @Override
    protected Class<? extends AbstractTimeWindowedAggFunc<?, ?>> getAggFunc() {
        return TimeWindowedValueCountsAggFunc.class;
    }
}

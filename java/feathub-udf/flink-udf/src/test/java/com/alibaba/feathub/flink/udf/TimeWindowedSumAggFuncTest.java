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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Test for {@link TimeWindowedSumAggFunc}. */
public class TimeWindowedSumAggFuncTest extends AbstractTimeWindowedAggFuncTest {

    @Test
    void testTimeWindowedSumAggFuncTinyInt() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, (byte) 1),
                        Row.of(0, (byte) 3),
                        Row.of(0, (byte) 5),
                        Row.of(0, (byte) 7));
        internalTest(DataTypes.TINYINT(), expected);
    }

    @Test
    void testTimeWindowedSumAggFuncSmallInt() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, (short) 1),
                        Row.of(0, (short) 3),
                        Row.of(0, (short) 5),
                        Row.of(0, (short) 7));
        internalTest(DataTypes.SMALLINT(), expected);
    }

    @Test
    void testTimeWindowedSumAggFuncInt() {
        List<Row> expected = Arrays.asList(Row.of(0, 1), Row.of(0, 3), Row.of(0, 5), Row.of(0, 7));
        internalTest(DataTypes.INT(), expected);
    }

    @Test
    void testTimeWindowedSumAggFuncBigInt() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1L), Row.of(0, 3L), Row.of(0, 5L), Row.of(0, 7L));
        internalTest(DataTypes.BIGINT(), expected);
    }

    @Test
    void testTimeWindowedSumAggFuncDecimal() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, DecimalDataUtils.castFrom(1, 38, 18).toBigDecimal()),
                        Row.of(0, DecimalDataUtils.castFrom(3, 38, 18).toBigDecimal()),
                        Row.of(0, DecimalDataUtils.castFrom(5, 38, 18).toBigDecimal()),
                        Row.of(0, DecimalDataUtils.castFrom(7, 38, 18).toBigDecimal()));
        internalTest(DataTypes.DECIMAL(10, 0), expected);
    }

    @Test
    void testTimeWindowedSumAggFuncFloat() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0f), Row.of(0, 3.0f), Row.of(0, 5.0f), Row.of(0, 7.0f));
        internalTest(DataTypes.FLOAT(), expected);
    }

    @Test
    void testTimeWindowedSumAggFuncDouble() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0d), Row.of(0, 3.0d), Row.of(0, 5.0d), Row.of(0, 7.0d));
        internalTest(DataTypes.DOUBLE(), expected);
    }

    @Override
    protected Class<? extends AbstractTimeWindowedAggFunc<?, ?>> getAggFunc() {
        return TimeWindowedSumAggFunc.class;
    }
}

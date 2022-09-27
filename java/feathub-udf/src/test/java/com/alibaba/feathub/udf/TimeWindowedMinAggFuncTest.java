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

package com.alibaba.feathub.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Test for {@link TimeWindowedMinAggFunc}. */
public class TimeWindowedMinAggFuncTest extends AbstractTimeWindowedAggFuncTest {

    @Test
    void testTimeWindowedMinAggFuncTinyInt() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, (byte) 1),
                        Row.of(0, (byte) 1),
                        Row.of(0, (byte) 2),
                        Row.of(0, (byte) 3));
        internalTest(DataTypes.TINYINT(), expected);
    }

    @Test
    void testTimeWindowedMinAggFuncSmallInt() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, (short) 1),
                        Row.of(0, (short) 1),
                        Row.of(0, (short) 2),
                        Row.of(0, (short) 3));
        internalTest(DataTypes.SMALLINT(), expected);
    }

    @Test
    void testTimeWindowedMinAggFuncInt() {
        List<Row> expected = Arrays.asList(Row.of(0, 1), Row.of(0, 1), Row.of(0, 2), Row.of(0, 3));
        internalTest(DataTypes.INT(), expected);
    }

    @Test
    void testTimeWindowedMinAggFuncBigInt() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1L), Row.of(0, 1L), Row.of(0, 2L), Row.of(0, 3L));
        internalTest(DataTypes.BIGINT(), expected);
    }

    @Test
    void testTimeWindowedMinAggFuncDecimal() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, DecimalDataUtils.castFrom(1, 38, 18).toBigDecimal()),
                        Row.of(0, DecimalDataUtils.castFrom(1, 38, 18).toBigDecimal()),
                        Row.of(0, DecimalDataUtils.castFrom(2, 38, 18).toBigDecimal()),
                        Row.of(0, DecimalDataUtils.castFrom(3, 38, 18).toBigDecimal()));
        internalTest(DataTypes.DECIMAL(10, 0), expected);
    }

    @Test
    void testTimeWindowedMinAggFuncFloat() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0f), Row.of(0, 1.0f), Row.of(0, 2.0f), Row.of(0, 3.0f));
        internalTest(DataTypes.FLOAT(), expected);
    }

    @Test
    void testTimeWindowedMinAggFuncDouble() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1.0d), Row.of(0, 1.0d), Row.of(0, 2.0d), Row.of(0, 3.0d));
        internalTest(DataTypes.DOUBLE(), expected);
    }

    @Override
    protected Class<? extends AbstractTimeWindowedAggFunc<?, ?>> getAggFunc() {
        return TimeWindowedMinAggFunc.class;
    }
}

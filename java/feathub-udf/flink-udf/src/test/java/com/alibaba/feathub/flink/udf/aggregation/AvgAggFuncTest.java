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

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;

import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AvgAggFunc}. */
class AvgAggFuncTest {
    @Test
    void testAvgAggregationFunctions() {
        innerTest(
                Arrays.array(1, 2, 3, 4),
                2.5,
                3.0,
                new AvgAggFunc<>(AggFuncUtils.getSumAggFunc(DataTypes.INT())));
        innerTest(
                Arrays.array(1L, 2L, 3L),
                2.0,
                2.5,
                new AvgAggFunc<>(AggFuncUtils.getSumAggFunc(DataTypes.BIGINT())));
        innerTest(
                Arrays.array(1.0f, 2.0f, 3.0f),
                2.0,
                2.5,
                new AvgAggFunc<>(AggFuncUtils.getSumAggFunc(DataTypes.FLOAT())));
        innerTest(
                Arrays.array(1.0, 2.0, 3.0),
                2.0,
                2.5,
                new AvgAggFunc<>(AggFuncUtils.getSumAggFunc(DataTypes.DOUBLE())));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void innerTest(
            Number[] inputs,
            Double expectedResult,
            Double expectedResultAfterRetract,
            AvgAggFunc aggFunc) {
        Tuple2<Long, ?> accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(null);
        for (Number input : inputs) {
            aggFunc.add(accumulator, input, 0);
        }
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResult);
        aggFunc.retract(accumulator, inputs[0]);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResultAfterRetract);
    }
}

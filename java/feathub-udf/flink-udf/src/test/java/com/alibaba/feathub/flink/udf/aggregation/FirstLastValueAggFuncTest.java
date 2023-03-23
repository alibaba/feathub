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

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.table.api.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FirstLastValueAggFunc}. */
class FirstLastValueAggFuncTest {
    @Test
    void tesFirstValue() {
        testFirstValueInternal(new FirstLastValueAggFunc<>(DataTypes.INT(), true), true);
        testFirstValueInternal(
                new FirstLastValueAggFuncWithoutRetract<>(DataTypes.INT(), true), false);
    }

    private void testFirstValueInternal(
            final FirstLastValueAggFunc<Integer> aggFunc, boolean testRetract) {
        final FirstLastValueAggFunc.FirstLastValueAccumulator<Integer> accumulator =
                aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 1, 1);
        aggFunc.add(accumulator, 2, 2);
        aggFunc.add(accumulator, 3, 3);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(0);
        if (testRetract) {
            aggFunc.retract(accumulator, 0);
            assertThat(aggFunc.getResult(accumulator)).isEqualTo(1);
        }
    }

    @Test
    void testLastValue() {
        testLastValueInternal(new FirstLastValueAggFunc<>(DataTypes.INT(), false), true);
        testLastValueInternal(
                new FirstLastValueAggFuncWithoutRetract<>(DataTypes.INT(), false), false);
    }

    private void testLastValueInternal(
            final FirstLastValueAggFunc<Integer> aggFunc, boolean testRetract) {
        final FirstLastValueAggFunc.FirstLastValueAccumulator<Integer> accumulator =
                aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 1, 1);
        aggFunc.add(accumulator, 2, 2);
        aggFunc.add(accumulator, 3, 3);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(3);
        if (testRetract) {
            aggFunc.retract(accumulator, 0);
            assertThat(aggFunc.getResult(accumulator)).isEqualTo(3);
        }
    }
}

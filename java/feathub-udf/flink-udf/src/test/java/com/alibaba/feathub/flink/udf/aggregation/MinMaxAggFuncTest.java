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

/** Test for {@link MinMaxAggFunc}. */
class MinMaxAggFuncTest {
    @Test
    void testMax() {
        testMaxInternal(new MinMaxAggFunc<>(DataTypes.INT(), false), true);
        testMaxInternal(new MinMaxAggFuncWithoutRetract<>(DataTypes.INT(), false), false);
    }

    private void testMaxInternal(final MinMaxAggFunc<Integer> aggFunc, boolean testRetract) {
        final MinMaxAggFunc.MinMaxAccumulator<Integer> accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 1, 0);
        aggFunc.add(accumulator, 3, 0);
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 4, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(4);
        if (testRetract) {
            aggFunc.retract(accumulator, 4);
            assertThat(aggFunc.getResult(accumulator)).isEqualTo(3);
        }
    }

    @Test
    void testMin() {
        testMinInternal(new MinMaxAggFunc<>(DataTypes.INT(), true), true);
        testMinInternal(new MinMaxAggFuncWithoutRetract<>(DataTypes.INT(), true), false);
    }

    private void testMinInternal(final MinMaxAggFunc<Integer> aggFunc, boolean testRetract) {
        final MinMaxAggFunc.MinMaxAccumulator<Integer> accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 1, 0);
        aggFunc.add(accumulator, 3, 0);
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 4, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(0);
        if (testRetract) {
            aggFunc.retract(accumulator, 0);
            assertThat(aggFunc.getResult(accumulator)).isEqualTo(1);
        }
    }
}

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

import org.apache.flink.table.api.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AggFuncWithLimit}. */
public class AggFuncWithLimitTest {
    @Test
    void testAggFuncWithLimit() {
        final MinMaxAggFunc<Integer> internalAggFunc = new MinMaxAggFunc<>(DataTypes.INT(), true);
        AggFuncWithLimit<Integer, Integer, MinMaxAggFunc.MinMaxAccumulator<Integer>> aggFunc =
                new AggFuncWithLimit<>(internalAggFunc, 3);
        innerTest(aggFunc, true);

        aggFunc = new AggFuncWithLimitWithoutRetract<>(internalAggFunc, 3);
        innerTest(aggFunc, false);
    }

    private static void innerTest(
            AggFuncWithLimit<Integer, Integer, MinMaxAggFunc.MinMaxAccumulator<Integer>> aggFunc,
            boolean testRetract) {
        AggFuncWithLimit.RawDataAccumulator<Integer> acc1 = aggFunc.createAccumulator();
        aggFunc.add(acc1, 1, 1L);
        aggFunc.add(acc1, 2, 2L);
        assertThat(aggFunc.getResult(acc1)).isEqualTo(1);

        AggFuncWithLimit.RawDataAccumulator<Integer> acc2 = aggFunc.createAccumulator();
        aggFunc.add(acc2, 3, 3L);
        aggFunc.add(acc2, 4, 4L);
        assertThat(aggFunc.getResult(acc2)).isEqualTo(3);

        AggFuncWithLimit.RawDataAccumulator<Integer> acc = aggFunc.createAccumulator();
        aggFunc.merge(acc, acc1);
        aggFunc.merge(acc, acc2);
        assertThat(aggFunc.getResult(acc)).isEqualTo(2);

        if (testRetract) {
            aggFunc.retractAccumulator(acc, acc1);
            assertThat(aggFunc.getResult(acc)).isEqualTo(3);
        }
    }
}

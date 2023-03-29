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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ValueCountsAggFunc}. */
class ValueCountsAggFuncTest {
    @Test
    void testValueCountsAggregationFunction() {
        final ValueCountsAggFunc aggFunc = new ValueCountsAggFunc(DataTypes.STRING());

        Map<Object, Long> accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();

        aggFunc.add(accumulator, "a", 0);
        aggFunc.add(accumulator, "a", 0);
        aggFunc.add(accumulator, "b", 0);
        Map<Object, Long> expectedResult = new HashMap<>();
        expectedResult.put("a", 2L);
        expectedResult.put("b", 1L);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResult);

        aggFunc.retract(accumulator, "b");
        Map<Object, Long> result = aggFunc.getResult(accumulator);
        assertThat(result).isEqualTo(Collections.singletonMap("a", 2L));

        aggFunc.add(accumulator, "b", 0);
        assertThat(result).isEqualTo(Collections.singletonMap("a", 2L));
    }
}

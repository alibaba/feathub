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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MergeValueCountsAggFunc}. */
class MergeValueCountsAggFuncTest {
    @Test
    void testMergeValueCountsAggregationFunction() {
        final MergeValueCountsAggFunc aggFunc =
                new MergeValueCountsAggFunc(DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()));
        final MergeValueCountsAggFunc.MergeValueCountsAccumulator accumulator =
                aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, Collections.singletonMap("a", 1L), 0);
        aggFunc.add(accumulator, Collections.singletonMap("a", 1L), 0);
        aggFunc.add(accumulator, Collections.singletonMap("b", 1L), 0);
        Map<Object, Long> expectedResult = new HashMap<>();
        expectedResult.put("a", 2L);
        expectedResult.put("b", 1L);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResult);
        aggFunc.retract(accumulator, Collections.singletonMap("b", 1L));
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(Collections.singletonMap("a", 2L));
    }
}

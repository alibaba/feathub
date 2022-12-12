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

/** Test for {@link LastValueAggFunc}. */
class LastValueAggFuncTest {
    @Test
    void testLastValueAggregationFunction() {
        final LastValueAggFunc<Integer> aggFunc = new LastValueAggFunc<>(DataTypes.INT());
        assertThat(aggFunc.getResult()).isNull();
        aggFunc.aggregate(1, 1);
        aggFunc.aggregate(2, 2);
        aggFunc.aggregate(0, 0);
        aggFunc.aggregate(3, 3);
        assertThat(aggFunc.getResult()).isEqualTo(3);
        aggFunc.reset();
        assertThat(aggFunc.getResult()).isNull();
    }
}

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
import org.apache.flink.types.Row;

import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowAvgAggFunc}. */
class RowAvgAggFuncTest {

    @Test
    void testAvgAggregationFunctions() {
        innerTest(
                Arrays.array(1, 2, 3, 4),
                2.5,
                new RowAvgAggFunc(DataTypes.ROW(DataTypes.INT(), DataTypes.BIGINT())));
        innerTest(
                Arrays.array(1L, 2L, 3L),
                2.0,
                new RowAvgAggFunc(DataTypes.ROW(DataTypes.BIGINT(), DataTypes.BIGINT())));
        innerTest(
                Arrays.array(1.0f, 2.0f, 3.0f),
                2.0,
                new RowAvgAggFunc(DataTypes.ROW(DataTypes.FLOAT(), DataTypes.BIGINT())));
        innerTest(
                Arrays.array(1.0, 2.0, 3.0),
                2.0,
                new RowAvgAggFunc(DataTypes.ROW(DataTypes.DOUBLE(), DataTypes.BIGINT())));
    }

    private void innerTest(Object[] inputs, Double expectedResult, RowAvgAggFunc aggFunc) {
        assertThat(aggFunc.getResult()).isEqualTo(null);
        for (Object input : inputs) {
            aggFunc.aggregate(Row.of(input, 1L), 0);
        }
        assertThat(aggFunc.getResult()).isEqualTo(expectedResult);
        aggFunc.reset();
        assertThat(aggFunc.getResult()).isEqualTo(null);
    }
}

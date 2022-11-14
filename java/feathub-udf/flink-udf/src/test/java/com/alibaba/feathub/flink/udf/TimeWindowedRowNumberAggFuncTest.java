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
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Test for {@link TimeWindowedRowNumberAggFunc}. */
public class TimeWindowedRowNumberAggFuncTest extends AbstractTimeWindowedAggFuncTest {
    @Test
    void testTimeWindowedRowNumberAggFuncInt() {
        List<Row> expected =
                Arrays.asList(Row.of(0, 1L), Row.of(0, 2L), Row.of(0, 2L), Row.of(0, 2L));
        internalTest(DataTypes.INT(), expected);
    }

    @Override
    protected Class<? extends AbstractTimeWindowedAggFunc<?, ?>> getAggFunc() {
        return TimeWindowedRowNumberAggFunc.class;
    }
}

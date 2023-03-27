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

package com.alibaba.feathub.flink.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;

/** Time windowed value counts aggregation function. */
public class TimeWindowedValueCountsAggFunc extends AbstractTimeWindowedAggFunc<Object, Object> {

    private final ValueCountsAggFunc valueCountsAggFunc = new ValueCountsAggFunc();

    @Override
    protected Object getValue(List<Object> values) {
        final Map<Object, Long> acc = valueCountsAggFunc.createAccumulator();
        for (Object value : values) {
            valueCountsAggFunc.accumulate(acc, value);
        }
        return valueCountsAggFunc.getValue(acc);
    }

    @Override
    protected DataType getResultDataType(DataType valueDataType) {
        return DataTypes.MAP(valueDataType, DataTypes.BIGINT());
    }
}

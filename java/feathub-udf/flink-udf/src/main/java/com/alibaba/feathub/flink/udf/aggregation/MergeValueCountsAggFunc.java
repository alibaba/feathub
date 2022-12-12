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

import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.Map;

/** Aggregate function that merge value counts. */
public class MergeValueCountsAggFunc implements AggFunc<Map<Object, Long>, Map<Object, Long>> {
    private final Map<Object, Long> valueCounts = new HashMap<>();
    private final DataType inDataType;

    public MergeValueCountsAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public void reset() {
        valueCounts.clear();
    }

    @Override
    public void aggregate(Map<Object, Long> value, long timestamp) {
        for (Map.Entry<Object, Long> entry : value.entrySet()) {
            final Object key = entry.getKey();
            valueCounts.put(key, valueCounts.getOrDefault(key, 0L) + entry.getValue());
        }
    }

    @Override
    public Map<Object, Long> getResult() {
        if (valueCounts.isEmpty()) {
            return null;
        }
        return valueCounts;
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }
}

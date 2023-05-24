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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.Map;

/** Aggregate function that counts the number of occurrences of input values. */
public class ValueCountsAggFunc implements AggFunc<Object, Map<Object, Long>, Map<Object, Long>> {
    private final DataType inDataType;

    public ValueCountsAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public void add(Map<Object, Long> accumulator, Object value, long timestamp) {
        accumulator.put(value, accumulator.getOrDefault(value, 0L) + 1);
    }

    @Override
    public void merge(Map<Object, Long> target, Map<Object, Long> source) {
        for (Map.Entry<Object, Long> entry : source.entrySet()) {
            final Object key = entry.getKey();
            target.put(key, target.getOrDefault(key, 0L) + entry.getValue());
        }
    }

    @Override
    public void retract(Map<Object, Long> accumulator, Object value) {
        long newCnt = accumulator.get(value) - 1;
        if (newCnt == 0) {
            accumulator.remove(value);
        } else {
            accumulator.put(value, newCnt);
        }
    }

    @Override
    public void retractAccumulator(Map<Object, Long> target, Map<Object, Long> source) {
        for (Map.Entry<Object, Long> entry : source.entrySet()) {
            final Object key = entry.getKey();
            long newCnt = target.get(key) - entry.getValue();
            if (newCnt == 0) {
                target.remove(key);
            } else {
                target.put(key, newCnt);
            }
        }
    }

    @Override
    public Map<Object, Long> getResult(Map<Object, Long> accumulator) {
        // TODO: change default value for empty window to empty map.
        if (accumulator.isEmpty()) {
            return null;
        }
        return new HashMap<>(accumulator);
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.MAP(inDataType, DataTypes.BIGINT());
    }

    @Override
    public Map<Object, Long> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public TypeInformation<Map<Object, Long>> getAccumulatorTypeInformation() {
        return Types.MAP(ExternalTypeInfo.of(inDataType), Types.LONG);
    }
}

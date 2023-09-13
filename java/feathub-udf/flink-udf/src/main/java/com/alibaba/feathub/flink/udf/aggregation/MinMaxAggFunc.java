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
import org.apache.flink.table.types.DataType;

import java.util.Map;
import java.util.TreeMap;

/** Aggregate function that get the min or max. */
public class MinMaxAggFunc<T extends Comparable<T>>
        implements AggFunc<T, T, MinMaxAggFunc.MinMaxAccumulator<T>> {

    private final DataType inDataType;
    protected final boolean isMin;

    public MinMaxAggFunc(DataType inDataType, boolean isMin) {
        this.inDataType = inDataType;
        this.isMin = isMin;
    }

    @Override
    public void add(MinMaxAccumulator<T> accumulator, T value, long timestamp) {
        accumulator.values.put(value, accumulator.values.getOrDefault(value, 0L) + 1);
    }

    @Override
    public void merge(MinMaxAccumulator<T> target, MinMaxAccumulator<T> source) {
        for (Map.Entry<T, Long> entry : source.values.entrySet()) {
            target.values.put(
                    entry.getKey(),
                    target.values.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
    }

    @Override
    public void retract(MinMaxAccumulator<T> accumulator, T value) {
        final long newCnt = accumulator.values.get(value) - 1;
        if (newCnt == 0) {
            accumulator.values.remove(value);
            return;
        }
        accumulator.values.put(value, newCnt);
    }

    @Override
    public void retractAccumulator(MinMaxAccumulator<T> target, MinMaxAccumulator<T> source) {
        for (Map.Entry<T, Long> entry : source.values.entrySet()) {
            final long newCnt = target.values.get(entry.getKey()) - entry.getValue();
            if (newCnt == 0) {
                target.values.remove(entry.getKey());
                continue;
            }
            target.values.put(entry.getKey(), newCnt);
        }
    }

    @Override
    public T getResult(MinMaxAccumulator<T> accumulator) {
        if (accumulator.values.isEmpty()) {
            return null;
        }

        if (isMin) {
            return accumulator.values.firstKey();
        } else {
            return accumulator.values.lastKey();
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public MinMaxAccumulator<T> createAccumulator() {
        return new MinMaxAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(MinMaxAccumulator.class);
    }

    /** Accumulator for {@link MinMaxAggFunc}. */
    public static class MinMaxAccumulator<T extends Comparable<T>> {
        public TreeMap<T, Long> values = new TreeMap<>();
    }
}

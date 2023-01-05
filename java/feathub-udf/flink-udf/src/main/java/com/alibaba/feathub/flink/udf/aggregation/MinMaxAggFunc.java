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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.types.DataType;

import java.util.TreeMap;

/** Aggregate function that get the min or max. */
public class MinMaxAggFunc<T extends Comparable<T>>
        implements AggFunc<T, T, MinMaxAggFunc.MinMaxAccumulator> {

    private final DataType inDataType;
    private final boolean isMin;

    public MinMaxAggFunc(DataType inDataType, boolean isMin) {
        this.inDataType = inDataType;
        this.isMin = isMin;
    }

    @Override
    public void add(MinMaxAccumulator accumulator, T value, long timestamp) {
        accumulator.values.put(value, accumulator.values.getOrDefault(value, 0L) + 1);
    }

    @Override
    public void retract(MinMaxAccumulator accumulator, T value) {
        final long newCnt = accumulator.values.get(value) - 1;
        if (newCnt == 0) {
            accumulator.values.remove(value);
            return;
        }
        accumulator.values.put(value, newCnt);
    }

    @Override
    public T getResult(MinMaxAccumulator accumulator) {
        if (accumulator.values.isEmpty()) {
            return null;
        }

        if (isMin) {
            return (T) accumulator.values.firstKey();
        } else {
            return (T) accumulator.values.lastKey();
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public MinMaxAccumulator createAccumulator() {
        return new MinMaxAccumulator();
    }

    @Override
    public TypeInformation<MinMaxAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(MinMaxAccumulator.class);
    }

    /** Accumulator for {@link MinMaxAggFunc}. */
    public static class MinMaxAccumulator {
        public final TreeMap<Comparable<?>, Long> values = new TreeMap<>();
    }
}

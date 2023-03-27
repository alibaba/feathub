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

import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

/**
 * Aggregate function that get the min or max.
 *
 * <p>Implementation is optimized based on the assumption that no retraction is required.
 */
public class MinMaxAggFuncWithoutRetract<T extends Comparable<T>> extends MinMaxAggFunc<T> {
    public MinMaxAggFuncWithoutRetract(DataType inDataType, boolean isMin) {
        super(inDataType, isMin);
    }

    @Override
    public void add(MinMaxAccumulator<T> accumulator, T value, long timestamp) {
        if (accumulator.values.isEmpty()) {
            accumulator.values.put(value, 1L);
            return;
        }

        T currentValue = Iterables.getOnlyElement(accumulator.values.keySet());

        if ((isMin && currentValue.compareTo(value) > 0)
                || (!isMin && currentValue.compareTo(value) < 0)) {
            accumulator.values.clear();
            accumulator.values.put(value, 1L);
        }
    }

    @Override
    public void merge(MinMaxAccumulator<T> target, MinMaxAccumulator<T> source) {
        add(target, Iterables.getOnlyElement(source.values.keySet()), -1L);
    }

    @Override
    public void retract(MinMaxAccumulator<T> accumulator, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void retractAccumulator(MinMaxAccumulator<T> target, MinMaxAccumulator<T> source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T getResult(MinMaxAccumulator<T> accumulator) {
        if (accumulator.values.isEmpty()) {
            return null;
        }
        return Iterables.getOnlyElement(accumulator.values.keySet());
    }
}

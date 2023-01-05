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
import org.apache.flink.util.Preconditions;

import java.util.LinkedList;

/**
 * Aggregate function that get the first value or last value.
 *
 * <p>It assumes that the values are ordered by time.
 */
public class FirstLastValueAggFunc<T>
        implements AggFunc<T, T, FirstLastValueAggFunc.FirstLastValueAccumulator> {

    private final DataType inDataType;
    private final boolean isFirstValue;

    public FirstLastValueAggFunc(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public void add(FirstLastValueAccumulator accumulator, T value, long timestamp) {
        Preconditions.checkState(
                timestamp > accumulator.lastTimestamp,
                "The value to the FirstLastValueAggFuncBase must be ordered by timestamp.");
        accumulator.lastTimestamp = timestamp;
        accumulator.values.add(value);
    }

    @Override
    public void retract(FirstLastValueAccumulator accumulator, T value) {
        Preconditions.checkState(
                accumulator.values.getFirst().equals(value),
                "Value must be retracted by the ordered as they added to the FirstLastValueAggFuncBase.");
        accumulator.values.removeFirst();
    }

    @Override
    public T getResult(FirstLastValueAccumulator accumulator) {
        if (accumulator.values.isEmpty()) {
            return null;
        }
        if (isFirstValue) {
            return (T) accumulator.values.getFirst();
        } else {
            return (T) accumulator.values.getLast();
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public FirstLastValueAccumulator createAccumulator() {
        return new FirstLastValueAccumulator();
    }

    @Override
    public TypeInformation<FirstLastValueAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(FirstLastValueAccumulator.class);
    }

    /** Accumulator for {@link FirstLastValueAggFunc}. */
    public static class FirstLastValueAccumulator {
        public final LinkedList<Object> values = new LinkedList<>();
        public long lastTimestamp = Long.MIN_VALUE;
    }
}

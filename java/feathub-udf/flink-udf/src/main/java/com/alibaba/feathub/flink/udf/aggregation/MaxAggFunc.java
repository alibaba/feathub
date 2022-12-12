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

/** Aggregation function that gets the maximum value. */
public class MaxAggFunc<T extends Comparable<T>> implements AggFunc<T, T> {
    private final DataType inDataType;
    private T maxValue = null;

    public MaxAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public void reset() {
        maxValue = null;
    }

    @Override
    public void aggregate(T value, long timestamp) {
        if (maxValue == null) {
            maxValue = value;
            return;
        }
        maxValue = maxValue.compareTo(value) < 0 ? value : maxValue;
    }

    @Override
    public T getResult() {
        return maxValue;
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }
}

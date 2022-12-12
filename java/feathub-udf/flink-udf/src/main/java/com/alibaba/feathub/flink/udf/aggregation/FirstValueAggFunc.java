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

/** Aggregation function that gets the first value. */
public class FirstValueAggFunc<T> implements AggFunc<T, T> {

    private final DataType inDataType;
    private T firstValue = null;
    private long minTimestamp = Long.MAX_VALUE;

    public FirstValueAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public void reset() {
        firstValue = null;
        minTimestamp = Long.MAX_VALUE;
    }

    @Override
    public void aggregate(T value, long timestamp) {
        if (timestamp >= minTimestamp) {
            return;
        }
        firstValue = value;
        minTimestamp = timestamp;
    }

    @Override
    public T getResult() {
        return firstValue;
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }
}

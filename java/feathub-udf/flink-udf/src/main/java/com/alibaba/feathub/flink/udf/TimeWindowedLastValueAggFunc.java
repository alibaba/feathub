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

import org.apache.flink.table.types.DataType;

import java.util.List;

/** Time windowed last value aggregation function. */
public class TimeWindowedLastValueAggFunc extends AbstractTimeWindowedAggFunc<Object, Object> {

    @Override
    public Object getValue(TimeWindowedAccumulator<Object> accumulator) {
        try {
            final List<Object> latestValues = accumulator.values.get(accumulator.latestTimestamp);
            if (latestValues == null || latestValues.size() < 1) {
                return null;
            }

            return latestValues.get(latestValues.size() - 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object getValue(List<Object> values) {
        throw new UnsupportedOperationException("This method should not be called.");
    }

    @Override
    protected DataType getResultDataType(DataType valueDataType) {
        return valueDataType;
    }
}

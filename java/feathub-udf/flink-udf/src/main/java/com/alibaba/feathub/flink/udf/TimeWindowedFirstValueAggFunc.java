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

import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;

/** Time windowed first value aggregation function. */
public class TimeWindowedFirstValueAggFunc extends AbstractTimeWindowedAggFunc<Object, Object> {

    @Override
    public Object getValue(TimeWindowedAccumulator<Object> accumulator) {
        final Long lowerBound = accumulator.latestTimestamp - timeInterval.toMillis();

        long earliestTimestamp = Long.MAX_VALUE;
        Object earliestValue = null;
        try {
            for (Map.Entry<Long, List<Object>> entry : accumulator.values.entries()) {
                if (entry.getKey() < lowerBound) {
                    continue;
                }

                if (entry.getKey() < earliestTimestamp) {
                    earliestTimestamp = entry.getKey();
                    earliestValue = entry.getValue().get(0);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return earliestValue;
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

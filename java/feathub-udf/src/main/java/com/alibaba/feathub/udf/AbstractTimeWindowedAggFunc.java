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

package com.alibaba.feathub.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for all the time windowed aggregation function.
 *
 * <p>The aggregate function should be called with a timeInterval. It only aggregates rows with row
 * time in the range of [rowTime - timeInterval, rowTime]. Currently, Flink SQL/Table only support
 * over window with either time range or row count-based range. This can be used with a row
 * count-based over window to achieve over window with both time range and row count-based range.
 *
 * @param <INPUT_T> The type of the input.
 * @param <RESULT_T> The result type of the aggregation.
 */
public abstract class AbstractTimeWindowedAggFunc<INPUT_T, RESULT_T>
        extends AggregateFunction<
                RESULT_T, AbstractTimeWindowedAggFunc.TimeWindowedAccumulator<INPUT_T>> {

    /** The time range of rows to include in the aggregation. */
    protected Duration timeInterval;

    @Override
    public RESULT_T getValue(TimeWindowedAccumulator<INPUT_T> accumulator) {
        final Long lowerBound = accumulator.latestTimestamp - timeInterval.toMillis();
        List<INPUT_T> values = new LinkedList<>();
        try {
            for (Map.Entry<Long, List<INPUT_T>> entry : accumulator.values.entries()) {
                if (entry.getKey() < lowerBound) {
                    continue;
                }
                values.addAll(entry.getValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getValue(values);
    }

    /**
     * Compute the aggregation result with the given value.
     *
     * @param values The values that are in the time window range.
     * @return The aggregation result.
     */
    protected abstract RESULT_T getValue(List<INPUT_T> values);

    @Override
    public TimeWindowedAccumulator<INPUT_T> createAccumulator() {
        return new TimeWindowedAccumulator<>();
    }

    public void accumulate(
            TimeWindowedAccumulator<INPUT_T> acc,
            Duration duration,
            INPUT_T value,
            LocalDateTime localDateTime)
            throws Exception {
        if (this.timeInterval == null) {
            this.timeInterval = duration;
        }
        Preconditions.checkState(
                this.timeInterval.equals(duration), "timeInterval should not changes.");

        final long timestamp = DateTimeUtils.toTimestampMillis(localDateTime);
        acc.addValue(value, timestamp);
    }

    public void retract(
            TimeWindowedAccumulator<INPUT_T> acc,
            Duration duration,
            INPUT_T value,
            LocalDateTime localDateTime)
            throws Exception {
        if (this.timeInterval == null) {
            this.timeInterval = duration;
        }
        Preconditions.checkState(
                this.timeInterval.equals(duration), "timeInterval should not changes.");

        final long timestamp = DateTimeUtils.toTimestampMillis(localDateTime);
        acc.removeValue(value, timestamp);
    }

    protected abstract DataType getResultDataType(DataType valueDataType);

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(
                        callContext ->
                                Optional.of(
                                        getResultDataType(
                                                callContext.getArgumentDataTypes().get(1))))
                .accumulatorTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.STRUCTURED(
                                                TimeWindowedAccumulator.class,
                                                DataTypes.FIELD(
                                                        "values",
                                                        MapView.newMapViewDataType(
                                                                DataTypes.BIGINT(),
                                                                DataTypes.ARRAY(
                                                                        callContext
                                                                                .getArgumentDataTypes()
                                                                                .get(1)))),
                                                DataTypes.FIELD(
                                                        "latestTimestamp", DataTypes.BIGINT()))))
                .build();
    }

    /**
     * The accumulator for {@link AbstractTimeWindowedAggFunc}.
     *
     * <p>The accumulator keeps track of the accumulated value and the timestamp of the latest
     * value.
     *
     * @param <VALUE_T> The type of the value in the accumulator.
     */
    public static class TimeWindowedAccumulator<VALUE_T> {
        public MapView<Long, List<VALUE_T>> values = new MapView<>();

        public Long latestTimestamp = 0L;

        public void addValue(VALUE_T value, Long timestamp) throws Exception {
            if (timestamp > latestTimestamp) {
                latestTimestamp = timestamp;
            }
            List<VALUE_T> vals = values.get(timestamp);
            if (vals == null) {
                vals = new LinkedList<>();
            }
            vals.add(value);
            values.put(timestamp, vals);
        }

        public void removeValue(VALUE_T value, Long timestamp) throws Exception {
            final List<VALUE_T> vals = values.get(timestamp);
            vals.remove(value);
            if (vals.size() == 0) {
                values.remove(timestamp);

                if (latestTimestamp.equals(timestamp)) {
                    resetLatestTimestamp();
                }
                return;
            }
            values.put(timestamp, vals);
        }

        private void resetLatestTimestamp() throws Exception {
            latestTimestamp = 0L;
            for (Map.Entry<Long, List<VALUE_T>> entry : values.entries()) {
                if (entry.getKey() > latestTimestamp) {
                    latestTimestamp = entry.getKey();
                }
            }
        }
    }
}

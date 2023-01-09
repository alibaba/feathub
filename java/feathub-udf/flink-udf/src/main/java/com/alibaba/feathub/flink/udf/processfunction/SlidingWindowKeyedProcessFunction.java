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

package com.alibaba.feathub.flink.udf.processfunction;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A KeyedProcessFunction that aggregate sliding windows with different sizes. The ProcessFunction
 * should be applied after the input has been aggregated with a tumble window with size equals to
 * the step size of sliding window.
 *
 * <p>One native Flink sliding window operator can only aggregate under one sliding window size. If
 * we need aggregation under different window sizes, we need to apply multiple native Flink window
 * operators with different sizes and join the results together. Each window operator keeps the rows
 * for its window size. This will result in a lots of duplicates row in the state backend. For
 * example, if we want to do aggregation under 1 hour and 2 hours, both sliding window will keep the
 * rows in the last one hour, which are duplicated. With this process function, we only keep the
 * rows for the maximum window size so that we can avoid duplicated rows in state backend.
 *
 * <p>The ProcessFunction assumes that: 1. rows of each key are ordered by the row time 2. row time
 * attribute of rows with the same key are all distinct. The assumptions hold true after applying
 * the tumbling window aggregation to the input with window size that is same as the step size of
 * the {@link SlidingWindowKeyedProcessFunction} to be applied.
 */
public class SlidingWindowKeyedProcessFunction extends KeyedProcessFunction<Row, Row, Row> {

    private final AggregationFieldsDescriptor aggregationFieldsDescriptor;
    private final TypeSerializer<Row> inputRowTypeSerializer;
    private final TypeSerializer<Row> outputRowTypeSerializer;
    private final String rowTimeFieldName;
    private final String[] keyFieldNames;
    private final long stepSizeMs;
    private SlidingWindowState state;
    private final PostSlidingWindowExpiredRowHandler expiredRowHandler;
    private final boolean skipSameWindowOutput;

    public SlidingWindowKeyedProcessFunction(
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            TypeSerializer<Row> inputRowTypeSerializer,
            TypeSerializer<Row> outputRowTypeSerializer,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            PostSlidingWindowExpiredRowHandler expiredRowHandler,
            boolean skipSameWindowOutput) {
        this.aggregationFieldsDescriptor = aggregationFieldsDescriptor;
        this.inputRowTypeSerializer = inputRowTypeSerializer;
        this.outputRowTypeSerializer = outputRowTypeSerializer;
        this.rowTimeFieldName = rowTimeFieldName;
        this.keyFieldNames = keyFieldNames;
        this.stepSizeMs = stepSizeMs;
        this.expiredRowHandler = expiredRowHandler;
        this.skipSameWindowOutput = skipSameWindowOutput;
    }

    @Override
    public void open(Configuration parameters) {
        state =
                SlidingWindowState.create(
                        getRuntimeContext(),
                        aggregationFieldsDescriptor,
                        inputRowTypeSerializer,
                        outputRowTypeSerializer);
    }

    @Override
    public void processElement(
            Row row, KeyedProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        final long rowTime = ((Instant) row.getFieldAs(rowTimeFieldName)).toEpochMilli();

        if (skipSameWindowOutput) {
            // Only register timer on the event time of the row and on the row expire time.
            ctx.timerService().registerEventTimeTimer(rowTime);
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggFieldDescriptor :
                    aggregationFieldsDescriptor.getAggFieldDescriptors()) {
                ctx.timerService()
                        .registerEventTimeTimer(rowTime + aggFieldDescriptor.windowSizeMs);
            }
        } else {
            Long triggerTime = state.maxRegisteredTimer.value();
            if (triggerTime == null) {
                triggerTime = rowTime;
            }
            long rowExpireTime = rowTime + aggregationFieldsDescriptor.getMaxWindowSizeMs();

            // TODO: Register at most one timer at a timestamp regardless of the number of the keys
            //  processed by the operator.
            ctx.timerService().registerEventTimeTimer(rowTime);
            ctx.timerService().registerEventTimeTimer(rowExpireTime);
            while (triggerTime <= rowExpireTime) {
                ctx.timerService().registerEventTimeTimer(triggerTime);
                triggerTime += stepSizeMs;
            }
            state.maxRegisteredTimer.update(triggerTime - stepSizeMs);
        }

        state.addRow(rowTime, row);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Row, Row, Row>.OnTimerContext ctx,
            Collector<Row> out)
            throws Exception {

        boolean hasRow = false;

        final List<Long> timestampList = state.getTimestampList();

        Row outputRow = Row.withNames();
        for (int i = 0; i < keyFieldNames.length; i++) {
            outputRow.setField(keyFieldNames[i], ctx.getCurrentKey().getField(i));
        }

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {

            final Object accumulator = state.getAccumulator(descriptor);

            Row rowToAdd = state.timestampToRow.get(timestamp);
            if (rowToAdd != null) {
                descriptor.aggFunc.add(
                        accumulator, rowToAdd.getField(descriptor.inFieldName), timestamp);
            }

            // Advance left idx and retract values
            int leftIdx;
            for (leftIdx = state.getLeftTimestampIdx(descriptor);
                    leftIdx < timestampList.size();
                    ++leftIdx) {
                long rowTime = timestampList.get(leftIdx);
                if (timestamp - descriptor.windowSizeMs < rowTime) {
                    if (rowTime <= timestamp) {
                        hasRow = true;
                    }
                    break;
                }
                Row curRow = state.timestampToRow.get(rowTime);
                descriptor.aggFunc.retract(accumulator, curRow.getField(descriptor.inFieldName));
            }

            outputRow.setField(descriptor.outFieldName, descriptor.aggFunc.getResult(accumulator));

            state.updateLeftTimestampIdx(descriptor, leftIdx);
            state.updateAccumulator(descriptor, accumulator);
        }
        outputRow.setField(rowTimeFieldName, Instant.ofEpochMilli(timestamp));

        state.pruneRow(timestamp, aggregationFieldsDescriptor);

        if (!hasRow) {
            state.maxRegisteredTimer.clear();
            if (expiredRowHandler == null) {
                // output nothing if no row is aggregated.
                return;
            }
            final Row lastRow = state.lastOutputRow.value();
            if (lastRow == null) {
                return;
            }
            expiredRowHandler.handleExpiredRow(out, lastRow, timestamp);
            state.lastOutputRow.clear();
            return;
        }

        state.lastOutputRow.update(outputRow);
        out.collect(outputRow);
    }

    /** The state of {@link SlidingWindowKeyedProcessFunction}. */
    private static class SlidingWindowState {

        /** This MapState map from row timestamp to the row. */
        private final MapState<Long, Row> timestampToRow;

        /**
         * This ListState keep all the row timestamp that has been added to the timestampToRow
         * state. Since the operator assume row are ordered by time, the timestamp list should be
         * ordered as well.
         */
        private final ListState<Long> timestampList;

        /**
         * This MapState map from output field name to the left index of the timestampList. If the
         * index is not null, it always points to the timestamp of the first row in the current
         * window. If it is null, it means there is no row in the current window.
         */
        private final MapState<String, Integer> outFieldNameToLeftTimestampIdx;

        /**
         * This ValueState keep the maximum registered timer so that we don't try to register the
         * same timer twice.
         */
        private final ValueState<Long> maxRegisteredTimer;

        /**
         * This ValueState keep the last output row so that we can handle last output row when it is
         * expired.
         */
        private final ValueState<Row> lastOutputRow;

        /** This is a map from output field name to the state of the corresponding accumulator. */
        private final Map<String, ValueState<Object>> outFieldNameToAccumulator;

        private SlidingWindowState(
                MapState<Long, Row> timestampToRow,
                ListState<Long> timestampList,
                MapState<String, Integer> outFieldNameToLeftTimestampIdx,
                ValueState<Long> maxRegisteredTimer,
                ValueState<Row> lastOutputRow,
                Map<String, ValueState<Object>> outFieldNameToAccumulator) {
            this.timestampToRow = timestampToRow;
            this.timestampList = timestampList;
            this.outFieldNameToLeftTimestampIdx = outFieldNameToLeftTimestampIdx;
            this.maxRegisteredTimer = maxRegisteredTimer;
            this.lastOutputRow = lastOutputRow;
            this.outFieldNameToAccumulator = outFieldNameToAccumulator;
        }

        public static SlidingWindowState create(
                RuntimeContext context,
                AggregationFieldsDescriptor aggregationFieldsDescriptor,
                TypeSerializer<Row> inputRowTypeSerializer,
                TypeSerializer<Row> outputRowTypeSerializer) {
            final MapState<Long, Row> timestampToRow =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "TimestampToRow",
                                    LongSerializer.INSTANCE,
                                    inputRowTypeSerializer));

            final ListState<Long> listState =
                    context.getListState(
                            new ListStateDescriptor<>(
                                    "TimestampListState", LongSerializer.INSTANCE));

            final MapState<String, Integer> outFieldNameToLeftTimestampIdx =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "OutFieldNameToLeftTimestampIdx",
                                    StringSerializer.INSTANCE,
                                    IntSerializer.INSTANCE));

            final ValueState<Long> maxRegisteredTimer =
                    context.getState(
                            new ValueStateDescriptor<>(
                                    "MaxRegisteredTimer", LongSerializer.INSTANCE));

            final ValueState<Row> lastOutputRow =
                    context.getState(
                            new ValueStateDescriptor<>("LastOutputRow", outputRowTypeSerializer));

            final Map<String, ValueState<Object>> outFieldNameToAccumulator = new HashMap<>();
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                    aggregationFieldsDescriptor.getAggFieldDescriptors()) {
                outFieldNameToAccumulator.put(
                        descriptor.outFieldName,
                        context.getState(
                                new ValueStateDescriptor<>(
                                        String.format("%sAccumulator", descriptor.outFieldName),
                                        descriptor.aggFunc.getAccumulatorTypeInformation())));
            }

            return new SlidingWindowState(
                    timestampToRow,
                    listState,
                    outFieldNameToLeftTimestampIdx,
                    maxRegisteredTimer,
                    lastOutputRow,
                    outFieldNameToAccumulator);
        }

        /**
         * Add the row to the state.
         *
         * @param timestamp The row time of the row.
         * @param row The row to be added
         */
        public void addRow(long timestamp, Row row) throws Exception {
            timestampToRow.put(timestamp, row);
            timestampList.add(timestamp);
        }

        /**
         * Prune all the row with timestamp that are less than or equals to currentTimestamp -
         * maxWindowSizeMs. It also cleans up the state if the window is empty after prune.
         *
         * @param currentTimestamp The current timestamp.
         * @param aggFieldsDescriptor The aggFieldsDescriptor.
         */
        public void pruneRow(long currentTimestamp, AggregationFieldsDescriptor aggFieldsDescriptor)
                throws Exception {
            long lowerBound = currentTimestamp - aggFieldsDescriptor.getMaxWindowSizeMs();
            List<Long> timestamps = getTimestampList();

            final int originalSize = timestamps.size();
            final Iterator<Long> iterator = timestamps.iterator();

            while (iterator.hasNext()) {
                final long cur = iterator.next();
                if (cur > lowerBound) {
                    break;
                }
                timestampToRow.remove(cur);
                iterator.remove();
            }

            for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                    aggFieldsDescriptor.getAggFieldDescriptors()) {
                final int removedRowCnt = originalSize - timestamps.size();
                int leftIdx = getLeftTimestampIdx(descriptor) - removedRowCnt;
                updateLeftTimestampIdx(descriptor, leftIdx);

                if (leftIdx >= timestamps.size() || timestamps.get(leftIdx) > currentTimestamp) {
                    outFieldNameToAccumulator.get(descriptor.outFieldName).clear();
                }
            }

            if (timestamps.isEmpty()) {
                timestampList.clear();
            } else {
                timestampList.update(timestamps);
            }
        }

        public int getLeftTimestampIdx(
                AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor)
                throws Exception {
            final Integer idx = outFieldNameToLeftTimestampIdx.get(descriptor.outFieldName);
            if (idx == null) {
                return 0;
            } else {
                return idx;
            }
        }

        public void updateLeftTimestampIdx(
                AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor, int idx)
                throws Exception {
            if (idx <= 0) {
                outFieldNameToLeftTimestampIdx.remove(descriptor.outFieldName);
            } else {
                outFieldNameToLeftTimestampIdx.put(descriptor.outFieldName, idx);
            }
        }

        public List<Long> getTimestampList() throws Exception {
            List<Long> timestamps = new ArrayList<>();
            final Iterable<Long> iter = timestampList.get();
            if (iter != null) {
                iter.forEach(timestamps::add);
            }
            return timestamps;
        }

        public Object getAccumulator(
                AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor)
                throws IOException {
            final ValueState<Object> accumulatorState =
                    outFieldNameToAccumulator.get(descriptor.outFieldName);
            Object acc = accumulatorState.value();
            if (acc == null) {
                acc = descriptor.aggFunc.createAccumulator();
                accumulatorState.update(acc);
            }
            return acc;
        }

        public void updateAccumulator(
                AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor,
                Object accumulator)
                throws IOException {
            outFieldNameToAccumulator.get(descriptor.outFieldName).update(accumulator);
        }
    }
}

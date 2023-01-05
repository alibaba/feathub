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
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    private final TypeSerializer<Row> rowTypeSerializer;
    private final TypeSerializer<Row> resultRowTypeSerializer;
    private final String rowTimeFieldName;
    private final String[] keyFieldNames;
    private final long stepSizeMs;
    private MultiWindowSizeState state;
    private final PostSlidingWindowExpiredRowHandler expiredRowHandler;
    private final boolean skipSameWindowOutput;

    public SlidingWindowKeyedProcessFunction(
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            TypeSerializer<Row> rowTypeSerializer,
            TypeSerializer<Row> resultRowTypeSerializer,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            PostSlidingWindowExpiredRowHandler expiredRowHandler,
            boolean skipSameWindowOutput) {
        this.aggregationFieldsDescriptor = aggregationFieldsDescriptor;
        this.rowTypeSerializer = rowTypeSerializer;
        this.resultRowTypeSerializer = resultRowTypeSerializer;
        this.rowTimeFieldName = rowTimeFieldName;
        this.keyFieldNames = keyFieldNames;
        this.stepSizeMs = stepSizeMs;
        this.expiredRowHandler = expiredRowHandler;
        this.skipSameWindowOutput = skipSameWindowOutput;
    }

    @Override
    public void open(Configuration parameters) {
        state =
                MultiWindowSizeState.create(
                        getRuntimeContext(), rowTypeSerializer, resultRowTypeSerializer);
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
        aggregationFieldsDescriptor.getAggFieldDescriptors().forEach(d -> d.aggFunc.reset());

        boolean hasRow = false;

        state.pruneRow(timestamp - aggregationFieldsDescriptor.getMaxWindowSizeMs());
        final Iterable<Long> timestampList = state.timestampList.get();
        for (long rowTime : timestampList) {
            if (rowTime > timestamp) {
                break;
            }
            Row curRow = state.timestampToRow.get(rowTime);

            // TODO: Optimize by supporting retract value from aggregation function.
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                    aggregationFieldsDescriptor.getAggFieldDescriptors()) {
                if (timestamp - descriptor.windowSizeMs >= rowTime) {
                    continue;
                }
                descriptor.aggFunc.aggregate(curRow.getField(descriptor.inFieldName), rowTime);
                hasRow = true;
            }
        }

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

        Row outputRow = Row.withNames();
        for (int i = 0; i < keyFieldNames.length; i++) {
            outputRow.setField(keyFieldNames[i], ctx.getCurrentKey().getField(i));
        }
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {
            outputRow.setField(descriptor.outFieldName, descriptor.aggFunc.getResult());
        }
        outputRow.setField(rowTimeFieldName, Instant.ofEpochMilli(timestamp));

        state.lastOutputRow.update(outputRow);
        out.collect(outputRow);
    }

    /** The state of {@link SlidingWindowKeyedProcessFunction}. */
    private static class MultiWindowSizeState {
        private final MapState<Long, Row> timestampToRow;

        private final ListState<Long> timestampList;

        private final ValueState<Long> maxRegisteredTimer;

        private final ValueState<Row> lastOutputRow;

        private MultiWindowSizeState(
                MapState<Long, Row> mapState,
                ListState<Long> timestampList,
                ValueState<Long> maxRegisteredTimer,
                ValueState<Row> lastOutputRow) {
            this.timestampToRow = mapState;
            this.timestampList = timestampList;
            this.maxRegisteredTimer = maxRegisteredTimer;
            this.lastOutputRow = lastOutputRow;
        }

        public static MultiWindowSizeState create(
                RuntimeContext context,
                TypeSerializer<Row> rowTypeSerializer,
                TypeSerializer<Row> outRowTypeSerializer) {
            final MapState<Long, Row> mapState =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "RowState", LongSerializer.INSTANCE, rowTypeSerializer));

            final ListState<Long> listState =
                    context.getListState(
                            new ListStateDescriptor<>(
                                    "TimestampListState", LongSerializer.INSTANCE));

            final ValueState<Long> maxRegisteredTimerState =
                    context.getState(
                            new ValueStateDescriptor<>(
                                    "MaxRegisteredTimerState", LongSerializer.INSTANCE));

            final ValueState<Row> lastOutputRow =
                    context.getState(
                            new ValueStateDescriptor<>("LastOutputRowState", outRowTypeSerializer));

            return new MultiWindowSizeState(
                    mapState, listState, maxRegisteredTimerState, lastOutputRow);
        }

        public void addRow(long timestamp, Row row) throws Exception {
            timestampToRow.put(timestamp, row);
            timestampList.add(timestamp);
        }

        public void pruneRow(long lowerBound) throws Exception {
            List<Long> timestamps = new ArrayList<>();
            final Iterable<Long> iter = timestampList.get();
            if (iter != null) {
                iter.forEach(timestamps::add);
            }

            final Iterator<Long> iterator = timestamps.iterator();

            while (iterator.hasNext()) {
                final long cur = iterator.next();
                if (cur > lowerBound) {
                    break;
                }
                timestampToRow.remove(cur);
                iterator.remove();
            }

            if (timestamps.isEmpty()) {
                timestampList.clear();
            } else {
                timestampList.update(timestamps);
            }
        }
    }
}

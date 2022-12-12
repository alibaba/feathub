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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
    private final TypeSerializer<Row> rowTypeSerializer;
    private final String rowTimeFieldName;
    private final long stepSizeMs;
    private MultiWindowSizeState state;

    public SlidingWindowKeyedProcessFunction(
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            TypeSerializer<Row> rowTypeSerializer,
            String rowTimeFieldName,
            long stepSizeMs) {
        this.aggregationFieldsDescriptor = aggregationFieldsDescriptor;
        this.rowTypeSerializer = rowTypeSerializer;
        this.rowTimeFieldName = rowTimeFieldName;
        this.stepSizeMs = stepSizeMs;
    }

    @Override
    public void open(Configuration parameters) {
        state = MultiWindowSizeState.create(getRuntimeContext(), rowTypeSerializer);
    }

    @Override
    public void processElement(
            Row row, KeyedProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        final long rowTime = ((Instant) row.getFieldAs(rowTimeFieldName)).toEpochMilli();
        long triggerTime = rowTime;
        while (triggerTime <= rowTime + aggregationFieldsDescriptor.getMaxWindowSizeMs()) {
            ctx.timerService().registerEventTimeTimer(triggerTime);
            triggerTime += stepSizeMs;
        }
        state.addRow(ctx.getCurrentKey(), rowTime, row);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Row, Row, Row>.OnTimerContext ctx,
            Collector<Row> out)
            throws Exception {
        aggregationFieldsDescriptor.getAggFieldDescriptors().forEach(d -> d.aggFunc.reset());

        boolean hasRow = false;

        for (long rowTime : state.keyToTimestamps.get(ctx.getCurrentKey())) {
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

        state.pruneRow(
                ctx.getCurrentKey(), timestamp - aggregationFieldsDescriptor.getMaxWindowSizeMs());

        if (!hasRow) {
            // output nothing if no row is aggregated.
            return;
        }

        final Row aggResultRow =
                Row.of(
                        aggregationFieldsDescriptor.getAggFieldDescriptors().stream()
                                .map(d -> d.aggFunc.getResult())
                                .toArray());

        out.collect(
                Row.join(
                        ctx.getCurrentKey(),
                        aggResultRow,
                        Row.of(Instant.ofEpochMilli(timestamp))));
    }

    /** The state of {@link SlidingWindowKeyedProcessFunction}. */
    private static class MultiWindowSizeState {
        private final MapState<Long, Row> timestampToRow;

        // One KeyedProcessFunction can process rows from different keys. Unlike KeyedState, which
        // is already scoped by key by Flink, we need to have a map that store the timestamp list
        // for each key.
        private final Map<Row, LinkedList<Long>> keyToTimestamps;

        private MultiWindowSizeState(MapState<Long, Row> mapState) {
            this.timestampToRow = mapState;
            this.keyToTimestamps = new HashMap<>();
        }

        public static MultiWindowSizeState create(
                RuntimeContext context, TypeSerializer<Row> rowTypeSerializer) {
            final MapState<Long, Row> mapState =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "RowState", LongSerializer.INSTANCE, rowTypeSerializer));

            return new MultiWindowSizeState(mapState);
        }

        public void addRow(Row key, long timestamp, Row row) throws Exception {
            LinkedList<Long> orderedTimestamp = keyToTimestamps.get(key);
            if (orderedTimestamp == null) {
                // Construct the timestamp list from the key of timestampToRow map state in case of
                // failure recovery.
                orderedTimestamp = new LinkedList<>();
                CollectionUtil.iterableToList(timestampToRow.keys()).stream()
                        .sorted()
                        .forEach(orderedTimestamp::add);
            }
            timestampToRow.put(timestamp, row);
            orderedTimestamp.addLast(timestamp);
            keyToTimestamps.put(key, orderedTimestamp);
        }

        public void pruneRow(Row key, long lowerBound) throws Exception {
            LinkedList<Long> orderedTimestamp = keyToTimestamps.get(key);
            if (orderedTimestamp == null) {
                return;
            }
            final Iterator<Long> iterator = orderedTimestamp.iterator();
            while (iterator.hasNext()) {
                final long cur = iterator.next();
                if (cur > lowerBound) {
                    break;
                }
                timestampToRow.remove(cur);
                iterator.remove();
            }
        }
    }
}

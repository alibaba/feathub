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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * The KeyedProcessFunction that is used after the Flink SQL Sliding Window operation by Feathub.
 *
 * <p>The function output the rows only when the result of the sliding window changes if
 * skipSameWindowOutput is true. And it handles the expired row with the given {@link
 * PostSlidingWindowExpiredRowHandler}.
 *
 * <p>It assumes that the rows of each key are ordered by the row time, which should hold true after
 * a sliding window.
 */
public class PostSlidingWindowKeyedProcessFunction extends KeyedProcessFunction<Row, Row, Row>
        implements ResultTypeQueryable<Row> {

    private final long windowStepSizeMs;
    private final TypeSerializer<Row> keySerializer;
    private final TypeSerializer<Row> rowTypeSerializer;
    private final ExternalTypeInfo<Row> rowTypeInfo;

    private final String rowTimeFieldName;
    private final PostSlidingWindowExpiredRowHandler expiredRowHandler;
    private final boolean skipSameWindowOutput;

    private MapState<Row, Tuple2<Long, Row>> lastRowState;

    public PostSlidingWindowKeyedProcessFunction(
            ResolvedSchema schema,
            long windowStepSizeMs,
            String[] keyFieldNames,
            String rowTimeFieldName,
            PostSlidingWindowExpiredRowHandler expiredRowHandler,
            boolean skipSameWindowOutput) {
        this.windowStepSizeMs = windowStepSizeMs;
        this.rowTimeFieldName = rowTimeFieldName;
        this.expiredRowHandler = expiredRowHandler;
        this.skipSameWindowOutput = skipSameWindowOutput;

        DataTypes.Field[] keyFields = new DataTypes.Field[keyFieldNames.length];
        for (int i = 0; i < keyFields.length; ++i) {
            final String keyFieldName = keyFieldNames[i];
            final Column keyCol =
                    schema.getColumn(keyFieldName)
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    String.format(
                                                            "The given key field %s doesn't exist.",
                                                            keyFieldName)));
            keyFields[i] = DataTypes.FIELD(keyCol.getName(), keyCol.getDataType());
        }

        this.keySerializer =
                ExternalTypeInfo.<Row>of(DataTypes.ROW(keyFields)).createSerializer(null);
        rowTypeInfo = ExternalTypeInfo.of(schema.toPhysicalRowDataType());
        this.rowTypeSerializer = rowTypeInfo.createSerializer(null);
    }

    @Override
    public void open(Configuration parameters) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        final TupleSerializer<Tuple2<Long, Row>> tupleSerializer =
                new TupleSerializer(
                        Tuple2.class,
                        new TypeSerializer[] {LongSerializer.INSTANCE, rowTypeSerializer});
        lastRowState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        "lastRowState", keySerializer, tupleSerializer));
    }

    @Override
    public void processElement(
            Row row, KeyedProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        final Row currentKey = ctx.getCurrentKey();
        final Tuple2<Long, Row> lastRowTuple = lastRowState.get(currentKey);
        final Instant rowInstant = row.getFieldAs(rowTimeFieldName);

        long currentRowTs = rowInstant.toEpochMilli();
        long currentRowExpirationTs = currentRowTs + windowStepSizeMs;
        ctx.timerService().registerEventTimeTimer(currentRowExpirationTs);

        if (lastRowTuple == null) {
            lastRowState.put(currentKey, new Tuple2<>(currentRowExpirationTs, row));
            out.collect(row);
            return;
        }

        final long lastRowExpirationTs = lastRowTuple.f0;
        final Row lastRow = lastRowTuple.f1;
        ctx.timerService().deleteEventTimeTimer(lastRowExpirationTs);

        if (lastRowExpirationTs < currentRowTs) {
            // The last row is expired.
            expiredRowHandler.handleExpiredRow(out, lastRow, lastRowExpirationTs);
        }

        // Do not output the current row if skipSameWindowOutput is true, last row is not expire
        // and the values do not change.
        if (skipSameWindowOutput
                && lastRowExpirationTs >= currentRowTs
                && isRowValueEquals(lastRow, row)) {
            // Only update the expiration timestamp
            lastRowState.put(currentKey, new Tuple2<>(currentRowExpirationTs, lastRow));
            return;
        }

        lastRowState.put(currentKey, new Tuple2<>(currentRowExpirationTs, row));
        out.collect(row);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Row, Row, Row>.OnTimerContext ctx,
            Collector<Row> out)
            throws Exception {
        final Row currentKey = ctx.getCurrentKey();
        final Tuple2<Long, Row> lastRowTuple = lastRowState.get(currentKey);

        expiredRowHandler.handleExpiredRow(out, lastRowTuple.f1, timestamp);
        lastRowState.remove(currentKey);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return rowTypeInfo;
    }

    private boolean isRowValueEquals(Row row1, Row row2) {
        row1 = Row.copy(row1);
        row2 = Row.copy(row2);
        row1.setField(rowTimeFieldName, null);
        row2.setField(rowTimeFieldName, null);

        return row1.equals(row2);
    }
}

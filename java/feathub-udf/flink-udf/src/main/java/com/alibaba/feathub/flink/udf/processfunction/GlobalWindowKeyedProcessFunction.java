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

package com.alibaba.feathub.flink.udf.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.feathub.flink.udf.processfunction.WindowUtils.hasEqualAggregationResult;

/**
 * A KeyedProcessFunction that aggregate rows up to the current row. The result is emitted as soon
 * as the result is updated.
 */
public class GlobalWindowKeyedProcessFunction extends KeyedProcessFunction<Row, Row, Row> {
    private static final long serialVersionUID = 1L;

    private final String[] keyFieldNames;
    private final AggregationFieldsDescriptor aggDescriptors;
    private final String rowTimeFieldName;
    private final TypeInformation<Row> resultTypeInfo;
    private final TypeInformation<Row> accTypeInfo;

    // state to hold the accumulators of the aggregations
    private transient ValueState<Row> accumulator;
    // state to hold the last output row
    private transient ValueState<Row> lastOutputRow;

    public GlobalWindowKeyedProcessFunction(
            String[] keyFieldNames,
            String rowTimeFieldName,
            AggregationFieldsDescriptor aggDescriptors,
            TypeInformation<Row> resultTypeInfo) {
        this.keyFieldNames = keyFieldNames;
        this.aggDescriptors = aggDescriptors;
        this.rowTimeFieldName = rowTimeFieldName;
        this.resultTypeInfo = resultTypeInfo;

        List<TypeInformation<?>> accumulatorFieldTypes = new ArrayList<>();
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggDescriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            accumulatorFieldTypes.add(
                    aggDescriptor.aggFuncWithoutRetract.getAccumulatorTypeInformation());
        }

        accTypeInfo = Types.ROW(accumulatorFieldTypes.toArray(new TypeInformation[0]));
    }

    @Override
    public void open(Configuration parameters) {

        // initialize accumulator state
        ValueStateDescriptor<Row> accStateDesc =
                new ValueStateDescriptor<>("accumulator", accTypeInfo);
        accumulator = getRuntimeContext().getState(accStateDesc);

        lastOutputRow =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<Row>("lastOutputRow", resultTypeInfo));
    }

    @Override
    public void processElement(
            Row row, KeyedProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {

        final Row currentKey = ctx.getCurrentKey();
        final long rowTime = ((Instant) row.getFieldAs(rowTimeFieldName)).toEpochMilli();

        Row acc = accumulator.value();
        if (acc == null) {
            acc = createAccumulator();
        }

        add(row, acc, rowTime);
        final Row outputRow = getResult(currentKey, rowTime, acc);
        if (!hasEqualAggregationResult(aggDescriptors, lastOutputRow.value(), outputRow)) {
            lastOutputRow.update(outputRow);
            out.collect(outputRow);
        }

        accumulator.update(acc);
    }

    private Row createAccumulator() {
        final Object[] accumulators =
                aggDescriptors.getAggFieldDescriptors().stream()
                        .map(descriptor -> descriptor.aggFuncWithoutRetract.createAccumulator())
                        .toArray();
        return Row.of(accumulators);
    }

    private void add(Row row, Row acc, Long timestamp) {
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            final int aggFieldIdx = aggDescriptors.getAggFieldIdx(descriptor);
            final Object fieldAcc = acc.getFieldAs(aggFieldIdx);
            Object fieldValue = row.getFieldAs(descriptor.fieldName);
            descriptor.aggFuncWithoutRetract.add(fieldAcc, fieldValue, timestamp);
        }
    }

    private Row getResult(Row key, Long timestamp, Row acc) {
        Row resRow = Row.withNames();

        for (int i = 0; i < keyFieldNames.length; i++) {
            resRow.setField(keyFieldNames[i], key.getField(i));
        }

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            final int aggFieldIdx = aggDescriptors.getAggFieldIdx(descriptor);
            Object fieldAcc = acc.getField(aggFieldIdx);
            resRow.setField(
                    descriptor.fieldName, descriptor.aggFuncWithoutRetract.getResult(fieldAcc));
        }

        resRow.setField(rowTimeFieldName, Instant.ofEpochMilli(timestamp));
        return resRow;
    }
}

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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.processfunction.SlidingWindowKeyedProcessFunction;
import com.alibaba.feathub.flink.udf.processfunction.SlidingWindowZeroValuedRowExpiredRowHandler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/** Utility methods to apply sliding windows. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SlidingWindowUtils {

    /**
     * Performs the pre-aggregation process with tumbling window before applying the sliding window
     * process function.
     *
     * @param tEnv The StreamTableEnvironment of the table.
     * @param table The input table.
     * @param windowDescriptor The descriptor of the sliding window to be applied to the input
     *     table.
     * @param aggDescriptors The aggregation field descriptor of the sliding window.
     * @param rowTimeFieldName The name of the row time field.
     */
    public static DataStream<Row> applySlidingWindowPreAggregationProcess(
            StreamTableEnvironment tEnv,
            Table table,
            SlidingWindowDescriptor windowDescriptor,
            AggregationFieldsDescriptor aggDescriptors,
            String rowTimeFieldName) {
        if (windowDescriptor.filterExpr != null) {
            table = table.filter(Expressions.callSql(windowDescriptor.filterExpr));
        }

        ResolvedSchema schema = table.getResolvedSchema();

        DataStream<Row> stream =
                tEnv.toChangelogStream(
                        table,
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        ChangelogMode.insertOnly());

        List<String> fieldNames = new ArrayList<>();
        List<TypeInformation<?>> resultFieldTypes = new ArrayList<>();
        List<TypeInformation<?>> accumulatorFieldTypes = new ArrayList<>();
        for (String key : windowDescriptor.groupByKeys) {
            fieldNames.add(key);
            resultFieldTypes.add(ExternalTypeInfo.of(getDataType(schema, key)));
            accumulatorFieldTypes.add(ExternalTypeInfo.of(getDataType(schema, key).nullable()));
        }
        fieldNames.add(rowTimeFieldName);
        resultFieldTypes.add(ExternalTypeInfo.of(DataTypes.TIMESTAMP_LTZ(3).notNull()));
        accumulatorFieldTypes.add(ExternalTypeInfo.of(DataTypes.TIMESTAMP_LTZ(3).nullable()));
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggDescriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            fieldNames.add(aggDescriptor.fieldName);
            resultFieldTypes.add(
                    aggDescriptor.aggFuncWithoutRetract.getAccumulatorTypeInformation());
            accumulatorFieldTypes.add(
                    aggDescriptor.aggFuncWithoutRetract.getAccumulatorTypeInformation());
        }

        long offset = TimeZone.getTimeZone(tEnv.getConfig().getLocalTimeZone()).getRawOffset();
        offset = getModdedOffset(windowDescriptor.stepSize.toMillis(), -offset);

        return stream.keyBy(
                        (KeySelector<Row, Object>)
                                value ->
                                        Row.of(
                                                windowDescriptor.groupByKeys.stream()
                                                        .map(value::getField)
                                                        .toArray()))
                .window(
                        TumblingEventTimeWindows.of(
                                Time.milliseconds(windowDescriptor.stepSize.toMillis()),
                                Time.milliseconds(offset)))
                .aggregate(
                        new SlidingWindowPreprocessAggregateFunction(
                                windowDescriptor.groupByKeys,
                                rowTimeFieldName,
                                aggDescriptors,
                                windowDescriptor.stepSize.toMillis(),
                                offset,
                                rowTimeFieldName),
                        Types.ROW_NAMED(
                                fieldNames.toArray(new String[0]),
                                accumulatorFieldTypes.toArray(new TypeInformation[0])),
                        Types.ROW_NAMED(
                                fieldNames.toArray(new String[0]),
                                resultFieldTypes.toArray(new TypeInformation[0])));
    }

    private static class SlidingWindowPreprocessAggregateFunction
            implements AggregateFunction<Row, Row, Row> {
        private final List<String> keyFields;
        private final String timestampField;
        private final AggregationFieldsDescriptor aggDescriptors;
        private final long size;
        private final long offset;
        private final String rowTimeFieldName;

        public SlidingWindowPreprocessAggregateFunction(
                List<String> keyFields,
                String timestampField,
                AggregationFieldsDescriptor aggDescriptors,
                long size,
                long offset,
                String rowTimeFieldName) {
            this.keyFields = keyFields;
            this.timestampField = timestampField;
            this.aggDescriptors = aggDescriptors;
            this.size = size;
            this.offset = offset;
            this.rowTimeFieldName = rowTimeFieldName;
        }

        @Override
        public Row createAccumulator() {
            Row acc = Row.withNames();
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                    aggDescriptors.getAggFieldDescriptors()) {
                acc.setField(
                        descriptor.fieldName, descriptor.aggFuncWithoutRetract.createAccumulator());
            }
            return acc;
        }

        @Override
        public Row add(Row row, Row acc) {
            long timestamp = ((Instant) row.getFieldAs(timestampField)).toEpochMilli();
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                    aggDescriptors.getAggFieldDescriptors()) {
                Object fieldValue = row.getFieldAs(descriptor.fieldName);
                Object fieldAcc = acc.getField(descriptor.fieldName);
                descriptor.aggFuncWithoutRetract.add(fieldAcc, fieldValue, timestamp);
            }

            if (acc.getField(rowTimeFieldName) == null) {
                acc.setField(
                        rowTimeFieldName,
                        Instant.ofEpochMilli(getWindowTime(timestamp, size, offset)));
                for (String key : keyFields) {
                    acc.setField(key, row.getField(key));
                }
            }

            return acc;
        }

        @Override
        public Row getResult(Row acc) {
            return acc;
        }

        @Override
        public Row merge(Row acc1, Row acc2) {
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                    aggDescriptors.getAggFieldDescriptors()) {
                Object fieldAcc1 = acc1.getField(descriptor.fieldName);
                Object fieldAcc2 = acc2.getField(descriptor.fieldName);
                descriptor.aggFuncWithoutRetract.merge(fieldAcc1, fieldAcc2);
            }
            if (acc1.getField(rowTimeFieldName) == null) {
                acc1.setField(rowTimeFieldName, acc2.getField(rowTimeFieldName));
                for (String key : keyFields) {
                    acc1.setField(key, acc2.getField(key));
                }
            }
            return acc1;
        }
    }

    private static long getModdedOffset(long stepSizeMs, long offsetMs) {
        offsetMs %= stepSizeMs;
        if (offsetMs < 0) {
            offsetMs += stepSizeMs;
        }
        return offsetMs;
    }

    private static long getWindowTime(long timestamp, long size, long offset) {
        long windowStart = TimeWindow.getWindowStartWithOffset(timestamp, offset % size, size);
        return windowStart + size - 1;
    }

    /**
     * Apply sliding window with the given step size to the given {@link Table} and {@link
     * AggregationFieldsDescriptor}. The {@link AggregationFieldsDescriptor} describes how to
     * compute each field. It includes the field name and data type of the input and output, the
     * size of the sliding window under which the aggregation performs, and the aggregation
     * function, e.g. SUM, AVG, etc.
     *
     * <p>The {@link SlidingWindowKeyedProcessFunction} is optimized to reduce the state usage when
     * computing aggregations under different sliding window sizes.
     *
     * @param tEnv The StreamTableEnvironment of the table.
     * @param rowDataStream The input stream containing pre-aggregated results.
     * @param dataTypeMap The map containing the data type of the output fields.
     * @param keyFieldNames The names of the group by keys for the sliding window.
     * @param rowTimeFieldName The name of the row time field.
     * @param stepSizeMs The step size of the sliding window in milliseconds.
     * @param aggregationFieldsDescriptor The descriptor of the aggregation field in the sliding
     *     window.
     * @param zeroValuedRow If the zeroValuedRow is not null, the sliding window will output a row
     *     with default value when the window is empty. The zeroValuedRow contains zero values of
     *     the all the fields, except row time field and key fields.
     * @param skipSameWindowOutput Whether to output if the sliding window output the same result.
     */
    public static Table applySlidingWindowAggregationProcess(
            StreamTableEnvironment tEnv,
            DataStream<Row> rowDataStream,
            Map<String, DataType> dataTypeMap,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            Row zeroValuedRow,
            boolean skipSameWindowOutput) {
        final TypeSerializer<Row> rowTypeSerializer =
                rowDataStream.getType().createSerializer(rowDataStream.getExecutionConfig());

        final List<DataTypes.Field> resultTableFields =
                getResultTableFields(
                        dataTypeMap, aggregationFieldsDescriptor, rowTimeFieldName, keyFieldNames);
        final List<String> resultTableFieldNames =
                resultTableFields.stream()
                        .map(DataTypes.AbstractField::getName)
                        .collect(Collectors.toList());
        final List<DataType> resultTableFieldDataTypes =
                resultTableFields.stream()
                        .map(DataTypes.Field::getDataType)
                        .collect(Collectors.toList());
        final ExternalTypeInfo<Row> resultRowTypeInfo =
                ExternalTypeInfo.of(
                        DataTypes.ROW(resultTableFields.toArray(new DataTypes.Field[0])));

        SlidingWindowZeroValuedRowExpiredRowHandler expiredRowHandler = null;
        if (zeroValuedRow != null) {
            expiredRowHandler =
                    new SlidingWindowZeroValuedRowExpiredRowHandler(
                            updateZeroValuedRow(
                                    zeroValuedRow,
                                    resultTableFieldNames,
                                    resultTableFieldDataTypes),
                            rowTimeFieldName,
                            keyFieldNames);
        }
        rowDataStream =
                rowDataStream
                        .keyBy(
                                (KeySelector<Row, Row>)
                                        value ->
                                                Row.of(
                                                        Arrays.stream(keyFieldNames)
                                                                .map(value::getField)
                                                                .toArray()))
                        .process(
                                new SlidingWindowKeyedProcessFunction(
                                        aggregationFieldsDescriptor,
                                        rowTypeSerializer,
                                        resultRowTypeInfo.createSerializer(null),
                                        keyFieldNames,
                                        rowTimeFieldName,
                                        stepSizeMs,
                                        expiredRowHandler,
                                        skipSameWindowOutput))
                        .returns(resultRowTypeInfo);

        Table table =
                tEnv.fromDataStream(
                        rowDataStream,
                        getResultTableSchema(
                                dataTypeMap,
                                aggregationFieldsDescriptor,
                                rowTimeFieldName,
                                keyFieldNames));
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggregationFieldDescriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {
            table =
                    table.addOrReplaceColumns(
                            $(aggregationFieldDescriptor.fieldName)
                                    .cast(aggregationFieldDescriptor.dataType)
                                    .as(aggregationFieldDescriptor.fieldName));
        }
        return table;
    }

    public static Row updateZeroValuedRow(
            Row zeroValuedRow, List<String> fieldNames, List<DataType> fieldDataType) {
        for (String fieldName : Objects.requireNonNull(zeroValuedRow.getFieldNames(true))) {
            final Object zeroValue = zeroValuedRow.getFieldAs(fieldName);
            if (zeroValue == null) {
                continue;
            }

            final int idx = fieldNames.indexOf(fieldName);
            if (idx == -1) {
                throw new RuntimeException(
                        String.format(
                                "The given default value of field %s doesn't exist.", fieldName));
            }
            final DataType dataType = fieldDataType.get(idx);
            final LogicalTypeRoot zeroValueType = dataType.getLogicalType().getTypeRoot();

            // Integer value pass as Integer type with PY4J from python to Java if the value is less
            // than Integer.MAX_VALUE. Floating point value pass as Double from python to Java.
            // Therefore, we need to cast to the corresponding data type of the column.
            switch (zeroValueType) {
                case INTEGER:
                case DOUBLE:
                    break;
                case BIGINT:
                    if (zeroValue instanceof Long) {
                        break;
                    } else if (zeroValue instanceof Number) {
                        final Number intValue = (Number) zeroValue;
                        zeroValuedRow.setField(fieldName, intValue.longValue());
                        break;
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unknown default value type %s for BIGINT column.",
                                        zeroValue.getClass().getName()));
                    }
                case FLOAT:
                    if (zeroValue instanceof Float) {
                        break;
                    } else if (zeroValue instanceof Number) {
                        final Number doubleValue = (Number) zeroValue;
                        zeroValuedRow.setField(fieldName, doubleValue.floatValue());
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unknown default value type %s for FLOAT column.",
                                        zeroValue.getClass().getName()));
                    }
                    break;
                default:
                    throw new RuntimeException(
                            String.format("Unknown default value type %s", zeroValueType));
            }
        }
        return zeroValuedRow;
    }

    private static List<DataTypes.Field> getResultTableFields(
            Map<String, DataType> dataTypeMap,
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            String rowTimeFieldName,
            String[] keyFieldNames) {
        List<DataTypes.Field> keyFields =
                Arrays.stream(keyFieldNames)
                        .map(fieldName -> DataTypes.FIELD(fieldName, dataTypeMap.get(fieldName)))
                        .collect(Collectors.toList());
        List<DataTypes.Field> aggFieldDataTypes =
                aggregationFieldsDescriptor.getAggFieldDescriptors().stream()
                        .map(d -> DataTypes.FIELD(d.fieldName, d.aggFunc.getResultDatatype()))
                        .collect(Collectors.toList());
        final List<DataTypes.Field> fields = new LinkedList<>();
        fields.addAll(keyFields);
        fields.addAll(aggFieldDataTypes);
        fields.add(DataTypes.FIELD(rowTimeFieldName, dataTypeMap.get(rowTimeFieldName)));
        return fields;
    }

    private static DataType getDataType(ResolvedSchema resolvedSchema, String fieldName) {
        return resolvedSchema
                .getColumn(fieldName)
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        String.format("Cannot find column %s.", fieldName)))
                .getDataType();
    }

    private static Schema getResultTableSchema(
            Map<String, DataType> dataTypeMap,
            AggregationFieldsDescriptor descriptor,
            String rowTimeFieldName,
            String[] keyFieldNames) {
        final Schema.Builder builder = Schema.newBuilder();

        for (String keyFieldName : keyFieldNames) {
            builder.column(keyFieldName, dataTypeMap.get(keyFieldName).notNull());
        }

        if (keyFieldNames.length > 0) {
            builder.primaryKey(keyFieldNames);
        }

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggregationFieldDescriptor :
                descriptor.getAggFieldDescriptors()) {
            builder.column(
                    aggregationFieldDescriptor.fieldName,
                    aggregationFieldDescriptor.aggFunc.getResultDatatype());
        }

        builder.column(rowTimeFieldName, dataTypeMap.get(rowTimeFieldName));

        // Records are ordered by row time after sliding window.
        builder.watermark(
                rowTimeFieldName,
                String.format("`%s` - INTERVAL '0.001' SECONDS", rowTimeFieldName));
        return builder.build();
    }
}

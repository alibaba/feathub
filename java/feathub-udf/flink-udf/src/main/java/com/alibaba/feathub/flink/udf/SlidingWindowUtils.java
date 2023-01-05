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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.processfunction.PostSlidingWindowZeroValuedRowExpiredRowHandler;
import com.alibaba.feathub.flink.udf.processfunction.SlidingWindowKeyedProcessFunction;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/** Utility methods to apply sliding windows. */
public class SlidingWindowUtils {

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
     * @param table The input table.
     * @param keyFieldNames The names of the group by keys for the sliding window.
     * @param rowTimeFieldName The name of the row time field.
     * @param stepSizeMs The step size of the sliding window in milliseconds.
     * @param aggregationFieldsDescriptor The descriptor of the aggregation field in the sliding
     *     window.
     */
    public static Table applySlidingWindowKeyedProcessFunction(
            StreamTableEnvironment tEnv,
            Table table,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            AggregationFieldsDescriptor aggregationFieldsDescriptor) {
        return applySlidingWindowKeyedProcessFunction(
                tEnv,
                table,
                keyFieldNames,
                rowTimeFieldName,
                stepSizeMs,
                aggregationFieldsDescriptor,
                null,
                false);
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
     * @param table The input table.
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
    public static Table applySlidingWindowKeyedProcessFunction(
            StreamTableEnvironment tEnv,
            Table table,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            Row zeroValuedRow,
            boolean skipSameWindowOutput) {
        final ResolvedSchema resolvedSchema = table.getResolvedSchema();
        DataStream<Row> rowDataStream =
                tEnv.toChangelogStream(
                        table,
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        ChangelogMode.all());

        final TypeSerializer<Row> rowTypeSerializer =
                ExternalTypeInfo.<Row>of(resolvedSchema.toPhysicalRowDataType())
                        .createSerializer(null);

        final List<DataTypes.Field> resultTableFields =
                getResultTableFields(
                        resolvedSchema,
                        aggregationFieldsDescriptor,
                        rowTimeFieldName,
                        keyFieldNames);
        final List<String> resultTableFieldNames =
                resultTableFields.stream()
                        .map(DataTypes.AbstractField::getName)
                        .collect(Collectors.toList());
        final List<DataType> resultTableFieldDataTypes =
                resultTableFields.stream()
                        .map(DataTypes.Field::getDataType)
                        .collect(Collectors.toList());
        final ExternalTypeInfo<Row> resultRowTypeInfo =
                ExternalTypeInfo.of(DataTypes.ROW(resultTableFields));

        PostSlidingWindowZeroValuedRowExpiredRowHandler expiredRowHandler = null;
        if (zeroValuedRow != null) {
            expiredRowHandler =
                    new PostSlidingWindowZeroValuedRowExpiredRowHandler(
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

        table =
                tEnv.fromDataStream(
                        rowDataStream,
                        getResultTableSchema(
                                resolvedSchema,
                                aggregationFieldsDescriptor,
                                rowTimeFieldName,
                                keyFieldNames));
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggregationFieldDescriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {
            table =
                    table.addOrReplaceColumns(
                            $(aggregationFieldDescriptor.outFieldName)
                                    .cast(aggregationFieldDescriptor.outDataType)
                                    .as(aggregationFieldDescriptor.outFieldName));
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
                    if (zeroValue instanceof Integer) {
                        final Integer intValue = (Integer) zeroValue;
                        zeroValuedRow.setField(fieldName, intValue.longValue());
                        break;
                    } else if (zeroValue instanceof Long) {
                        break;
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unknown default value type %s for BIGINT column.",
                                        zeroValue.getClass().getName()));
                    }
                case FLOAT:
                    if (zeroValue instanceof Double) {
                        final Double doubleValue = (Double) zeroValue;
                        zeroValuedRow.setField(fieldName, doubleValue.floatValue());
                    } else if (zeroValue instanceof Float) {
                        break;
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
            ResolvedSchema resolvedSchema,
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            String rowTimeFieldName,
            String[] keyFieldNames) {
        List<DataTypes.Field> keyFields =
                Arrays.stream(keyFieldNames)
                        .map(
                                fieldName ->
                                        DataTypes.FIELD(
                                                fieldName, getDataType(resolvedSchema, fieldName)))
                        .collect(Collectors.toList());
        List<DataTypes.Field> aggFieldDataTypes =
                aggregationFieldsDescriptor.getAggFieldDescriptors().stream()
                        .map(d -> DataTypes.FIELD(d.outFieldName, d.aggFunc.getResultDatatype()))
                        .collect(Collectors.toList());
        final List<DataTypes.Field> fields = new LinkedList<>();
        fields.addAll(keyFields);
        fields.addAll(aggFieldDataTypes);
        fields.add(
                DataTypes.FIELD(rowTimeFieldName, getDataType(resolvedSchema, rowTimeFieldName)));
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
            ResolvedSchema resolvedSchema,
            AggregationFieldsDescriptor descriptor,
            String rowTimeFieldName,
            String[] keyFieldNames) {
        final Schema.Builder builder = Schema.newBuilder();

        for (String keyFieldName : keyFieldNames) {
            builder.column(keyFieldName, getDataType(resolvedSchema, keyFieldName).notNull());
        }

        if (keyFieldNames.length > 0) {
            builder.primaryKey(keyFieldNames);
        }

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggregationFieldDescriptor :
                descriptor.getAggFieldDescriptors()) {
            builder.column(
                    aggregationFieldDescriptor.outFieldName,
                    aggregationFieldDescriptor.aggFunc.getResultDatatype());
        }

        builder.column(rowTimeFieldName, getDataType(resolvedSchema, rowTimeFieldName));

        // Records are ordered by row time after sliding window.
        builder.watermark(
                rowTimeFieldName,
                String.format("`%s` - INTERVAL '0.001' SECONDS", rowTimeFieldName));
        return builder.build();
    }
}

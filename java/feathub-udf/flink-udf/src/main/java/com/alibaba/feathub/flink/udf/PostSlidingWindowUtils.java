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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

/** Utility method to be used by Feathub after sliding window. */
public class PostSlidingWindowUtils {

    /**
     * Apply the {@link PostSlidingWindowKeyedProcessFunction} to the given table. A row with
     * default value is sent when a result is expired. If skipSameWindowOutput is false, rows are
     * only emitted when the values are changed.
     *
     * @param tEnv The StreamTableEnvironment of the table.
     * @param table The input table.
     * @param windowStepSizeMs The step size of the sliding window that the input table has been
     *     applied.
     * @param defaultRow The default row that contains default values of the all the fields, except
     *     row time field and key fields.
     * @param skipSameWindowOutput Whether to output if the sliding window output the same result.
     * @param rowTimeFieldName The name of the row time field.
     * @param keyFieldNames The names of the group by keys of the sliding window that the input
     *     table has been applied.
     * @return The result table.
     */
    public static Table postSlidingWindow(
            StreamTableEnvironment tEnv,
            Table table,
            long windowStepSizeMs,
            Row defaultRow,
            boolean skipSameWindowOutput,
            String rowTimeFieldName,
            String... keyFieldNames) {
        final ResolvedSchema resolvedSchema = table.getResolvedSchema();
        DataStream<Row> rowDataStream =
                tEnv.toChangelogStream(
                        table,
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        ChangelogMode.all());

        for (String fieldName : Objects.requireNonNull(defaultRow.getFieldNames(true))) {
            final Object defaultValue = defaultRow.getFieldAs(fieldName);
            if (defaultValue == null) {
                continue;
            }

            final DataType dataType =
                    resolvedSchema
                            .getColumn(fieldName)
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    String.format(
                                                            "The given default value of field %s doesn't exist.",
                                                            fieldName)))
                            .getDataType();
            final LogicalTypeRoot defaultValueType = dataType.getLogicalType().getTypeRoot();

            // Integer value pass as Integer type with PY4J from python to Java if the value is less
            // than Integer.MAX_VALUE. Floating point value pass as Double from python to Java.
            // Therefore, we need to cast to the corresponding data type of the column.
            switch (defaultValueType) {
                case INTEGER:
                case DOUBLE:
                    break;
                case BIGINT:
                    if (defaultValue instanceof Integer) {
                        final Integer intValue = (Integer) defaultValue;
                        defaultRow.setField(fieldName, intValue.longValue());
                        break;
                    } else if (defaultValue instanceof Long) {
                        break;
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unknown default value type %s for BIGINT column.",
                                        defaultValue.getClass().getName()));
                    }
                case FLOAT:
                    if (defaultValue instanceof Double) {
                        final Double doubleValue = (Double) defaultValue;
                        defaultRow.setField(fieldName, doubleValue.floatValue());
                    } else if (defaultValue instanceof Float) {
                        break;
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unknown default value type %s for FLOAT column.",
                                        defaultValue.getClass().getName()));
                    }
                    break;
                default:
                    throw new RuntimeException(
                            String.format("Unknown default value type %s", defaultValueType));
            }
        }

        rowDataStream =
                rowDataStream
                        .keyBy(new PostWindowKeySelector(keyFieldNames))
                        .process(
                                new PostSlidingWindowKeyedProcessFunction(
                                        resolvedSchema,
                                        windowStepSizeMs,
                                        keyFieldNames,
                                        rowTimeFieldName,
                                        new PostSlidingWindowDefaultRowExpiredRowHandler(
                                                defaultRow, rowTimeFieldName, keyFieldNames),
                                        skipSameWindowOutput))
                        .name(
                                String.format(
                                        "PostSlidingWindow[keys=%s, stepSizeMs=%s, emptyWindowOutput=default_value, skipSameWindowOutput=%s]",
                                        Arrays.toString(keyFieldNames),
                                        windowStepSizeMs,
                                        skipSameWindowOutput));
        return tEnv.fromDataStream(
                rowDataStream,
                getResultTableSchema(resolvedSchema, rowTimeFieldName, keyFieldNames));
    }

    private static Schema getResultTableSchema(
            ResolvedSchema resolvedSchema, String rowTimeFieldName, String[] keyFieldNames) {
        final HashSet<String> keyNames = new HashSet<>(Arrays.asList(keyFieldNames));
        final Schema.Builder builder = Schema.newBuilder();
        for (Column column : resolvedSchema.getColumns()) {
            String colName = column.getName();
            DataType dataType = column.getDataType();
            if (keyNames.contains(colName)) {
                // key fields cannot not be null.
                dataType = dataType.notNull();
            }
            builder.column(colName, dataType);
        }

        if (keyFieldNames.length > 0) {
            builder.primaryKey(keyFieldNames);
        }

        // Records are ordered by row time after sliding window.
        builder.watermark(
                rowTimeFieldName, String.format("`%s` - INTERVAL '0' SECONDS", rowTimeFieldName));
        return builder.build();
    }

    private static Row getKeyRow(Row row, String[] keyFieldNames) {
        Object[] keyValues = new Object[keyFieldNames.length];
        for (int i = 0; i < keyFieldNames.length; ++i) {
            keyValues[i] = row.getFieldAs(keyFieldNames[i]);
        }
        return Row.of(keyValues);
    }

    /** KeySelector for post sliding window function. */
    public static class PostWindowKeySelector implements KeySelector<Row, Row> {
        private final String[] keyFieldNames;

        public PostWindowKeySelector(String... keyFieldNames) {
            this.keyFieldNames = keyFieldNames;
        }

        @Override
        public Row getKey(Row row) {
            return getKeyRow(row, keyFieldNames);
        }
    }
}

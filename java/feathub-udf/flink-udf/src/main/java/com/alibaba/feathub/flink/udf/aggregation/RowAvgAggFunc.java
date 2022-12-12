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

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import java.util.List;

/** Aggregation function that calculates the average of rows with sum and count. */
public class RowAvgAggFunc implements AggFunc<Row, Double> {

    private final DataType valueType;
    Row avgRow = null;

    public RowAvgAggFunc(DataType valueType) {
        final List<DataType> childrenType = valueType.getChildren();
        if (childrenType.size() != 2
                || (!childrenType
                        .get(1)
                        .getLogicalType()
                        .getTypeRoot()
                        .equals(LogicalTypeRoot.BIGINT))) {
            throw new RuntimeException(
                    "RowAvgAggregationFunction expect Row with two fields and the second fields has to be BIGINT.");
        }
        this.valueType = childrenType.get(0);
    }

    @Override
    public void reset() {
        avgRow = null;
    }

    @Override
    public void aggregate(Row value, long timestamp) {
        if (avgRow == null) {
            avgRow = value;
            return;
        }

        mergeRow(value);
    }

    private void mergeRow(Row value) {
        Object sum1 = avgRow.getField(0);
        Object sum2 = value.getField(0);
        Long cnt1 = avgRow.getFieldAs(1);
        Long cnt2 = value.getFieldAs(1);

        if (sum1 == null || sum2 == null || cnt1 == null || cnt2 == null) {
            throw new RuntimeException("sum and count cannot be null.");
        }

        if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
            avgRow = Row.of((Integer) sum1 + (Integer) sum2, cnt1 + cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
            avgRow = Row.of((Long) sum1 + (Long) sum2, cnt1 + cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.FLOAT)) {
            avgRow = Row.of((Float) sum1 + (Float) sum2, cnt1 + cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DOUBLE)) {
            avgRow = Row.of((Double) sum1 + (Double) sum2, cnt1 + cnt2);
        } else {
            throw new RuntimeException(
                    String.format("Unsupported type for AvgAggregationFunction %s.", valueType));
        }
    }

    @Override
    public Double getResult() {
        if (avgRow == null) {
            return null;
        }
        Object sum = avgRow.getField(0);
        Long cnt = avgRow.getFieldAs(1);

        if (sum == null || cnt == null) {
            throw new RuntimeException("sum and count cannot be null.");
        }

        if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
            return ((Integer) sum) * 1.0 / cnt;
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
            return ((Long) sum) * 1.0 / cnt;
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.FLOAT)) {
            return (double) (((Float) sum) / cnt);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DOUBLE)) {
            return ((Double) sum) / cnt;
        } else {
            throw new RuntimeException(
                    String.format("Unsupported type for AvgAggregationFunction %s.", valueType));
        }
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.DOUBLE();
    }
}

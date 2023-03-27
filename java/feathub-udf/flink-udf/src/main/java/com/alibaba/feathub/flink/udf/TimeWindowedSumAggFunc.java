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

import org.apache.flink.table.types.DataType;

import java.math.BigDecimal;
import java.util.List;

/** Time windowed sum aggregation function. */
public class TimeWindowedSumAggFunc extends AbstractTimeWindowedAggFunc<Object, Object> {

    @Override
    protected Object getValue(List<Object> values) {
        if (values.size() == 0) {
            return null;
        }

        final Object val = values.get(0);
        if (val instanceof Byte) {
            return (byte) values.stream().mapToInt(i -> ((Byte) i).intValue()).sum();
        } else if (val instanceof Short) {
            return (short) values.stream().mapToInt(i -> ((Short) i).intValue()).sum();
        } else if (val instanceof Integer) {
            return values.stream().mapToInt(i -> (Integer) i).sum();
        } else if (val instanceof Long) {
            return values.stream().mapToLong(i -> (Long) i).sum();
        } else if (val instanceof BigDecimal) {
            return values.stream().reduce((o1, o2) -> ((BigDecimal) o1).add((BigDecimal) o2)).get();
        } else if (val instanceof Float) {
            return (float) values.stream().mapToDouble(i -> (Float) i).sum();
        } else if (val instanceof Double) {
            return values.stream().mapToDouble(i -> (Double) i).sum();
        } else {
            throw new RuntimeException(
                    String.format(
                            "Unsupported type for TimeWindowedSumAggFunc %s.",
                            val.getClass().getName()));
        }
    }

    @Override
    protected DataType getResultDataType(DataType valueDataType) {
        return valueDataType;
    }
}

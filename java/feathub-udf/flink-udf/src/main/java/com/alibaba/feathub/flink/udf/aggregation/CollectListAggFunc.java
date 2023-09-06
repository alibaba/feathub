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

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

/** An aggregate function that collects values into a list. */
public class CollectListAggFunc extends RawDataAccumulatingAggFunc<Object, Object> {
    private final DataType inDataType;

    public CollectListAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public Object getResult(RawDataAccumulator<Object> accumulator) {
        return toArray(
                accumulator.rawDataList.stream().map(x -> x.f0).collect(Collectors.toList()));
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.ARRAY(inDataType);
    }

    public static Object toArray(List<Object> values) {
        if (values.isEmpty()) {
            return new Object[0];
        }

        if (values.get(0) instanceof Long) {
            return values.toArray(new Long[0]);
        } else if (values.get(0) instanceof Integer) {
            return values.toArray(new Integer[0]);
        } else if (values.get(0) instanceof Double) {
            return values.toArray(new Double[0]);
        } else if (values.get(0) instanceof Float) {
            return values.toArray(new Float[0]);
        } else if (values.get(0) instanceof String) {
            return values.toArray(new String[0]);
        }

        throw new RuntimeException(
                String.format(
                        "Unsupported type for COLLECT_LIST %s.",
                        values.get(0).getClass().getName()));
    }
}

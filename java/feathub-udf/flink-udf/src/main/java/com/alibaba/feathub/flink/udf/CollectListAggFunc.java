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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** An aggregate function that collects values into a list. */
// TODO: Update this function to reuse the implementations of
//  com.alibaba.feathub.flink.udf.aggregation.CollectListAggFunc.
public class CollectListAggFunc extends AggregateFunction<Object, CollectListAggFunc.Accumulator> {

    @Override
    public Object getValue(Accumulator acc) {
        return toArray(acc.values);
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

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator acc, Object value) {
        acc.values.add(value);
    }

    public void retract(Accumulator acc, Object value) {
        acc.values.remove(value);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .accumulatorTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.RAW(
                                                Accumulator.class,
                                                TypeInformation.of(Accumulator.class)
                                                        .createSerializer(new ExecutionConfig()))))
                .outputTypeStrategy(
                        callContext -> {
                            final DataType argumentDataType =
                                    callContext.getArgumentDataTypes().get(0);
                            return Optional.of(DataTypes.ARRAY(argumentDataType));
                        })
                .build();
    }

    /** Accumulator for COLLECT_LIST. */
    public static class Accumulator {
        List<Object> values = new ArrayList<>();
    }
}

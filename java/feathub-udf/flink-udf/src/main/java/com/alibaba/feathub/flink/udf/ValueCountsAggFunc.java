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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** An aggregate function that counts each unique value and returns as Map. */
// TODO: Update this function to reuse the implementations of
//  com.alibaba.feathub.flink.udf.aggregation.ValueCountsAggFunc
public class ValueCountsAggFunc extends AggregateFunction<Map<Object, Long>, Map<Object, Long>> {

    public Map<Object, Long> getValue(Map<Object, Long> accumulator) {
        return accumulator;
    }

    public Map<Object, Long> createAccumulator() {
        return new HashMap<>();
    }

    public void accumulate(Map<Object, Long> acc, Object value) {
        acc.put(value, acc.getOrDefault(value, 0L) + 1);
    }

    public void retract(Map<Object, Long> acc, Object value) {
        if (!acc.containsKey(value)) {
            throw new RuntimeException(String.format("Retracting a non-exist key %s", value));
        }

        final long cnt = acc.get(value) - 1;
        if (cnt <= 0) {
            acc.remove(value);
            return;
        }

        acc.put(value, cnt);
    }

    public void merge(Map<Object, Long> acc, Iterable<Map<Object, Long>> it) {
        for (Map<Object, Long> a : it) {
            for (Map.Entry<Object, Long> entry : a.entrySet()) {
                acc.put(entry.getKey(), acc.getOrDefault(entry.getKey(), 0L) + entry.getValue());
            }
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(
                        callContext -> {
                            final DataType argumentDataType =
                                    callContext.getArgumentDataTypes().get(0);
                            return Optional.of(DataTypes.MAP(argumentDataType, DataTypes.BIGINT()));
                        })
                .build();
    }
}

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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import java.time.Instant;
import java.util.Optional;

/** Adaptor class that adapts the {@link AggFunc} to Flink {@link AggregateFunction}. */
public class AggFuncAdaptor<IN_T, OUT_T, ACC_T> extends AggregateFunction<OUT_T, ACC_T> {

    private final AggFunc<IN_T, OUT_T, ACC_T> innerAggFunc;

    public AggFuncAdaptor(AggFunc<IN_T, OUT_T, ACC_T> innerAggFunc) {
        this.innerAggFunc = innerAggFunc;
    }

    public void accumulate(ACC_T acc, IN_T value, Instant instant) throws Exception {
        final long timestamp = instant.toEpochMilli();
        innerAggFunc.add(acc, value, timestamp);
    }

    public void retract(ACC_T acc, IN_T value, Instant instant) throws Exception {
        innerAggFunc.retract(acc, value);
    }

    @Override
    public OUT_T getValue(ACC_T accumulator) {
        return innerAggFunc.getResult(accumulator);
    }

    @Override
    public ACC_T createAccumulator() {
        return innerAggFunc.createAccumulator();
    }

    public DataType getResultDataType() {
        return innerAggFunc.getResultDatatype();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(callContext -> Optional.of(getResultDataType()))
                .accumulatorTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.of(innerAggFunc.getAccumulatorTypeInformation())
                                                .toDataType(callContext.getDataTypeFactory())))
                .build();
    }
}

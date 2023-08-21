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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/** Aggregation function decorator that only aggregates records with filter flag set to true. */
public class AggFuncWithFilterFlag<IN_T, OUT_T, ACC_T> implements AggFunc<Row, OUT_T, ACC_T> {

    private final AggFunc<IN_T, OUT_T, ACC_T> innerAggFunc;

    public AggFuncWithFilterFlag(AggFunc<IN_T, OUT_T, ACC_T> innerAggFunc) {
        this.innerAggFunc = innerAggFunc;
    }

    @Override
    public void add(ACC_T accumulator, Row value, long timestamp) {
        Preconditions.checkArgument(
                value.getArity() == 2, "Row to AggFuncWithFilterFlag can only have 2 columns.");
        Boolean filterFlag = value.getFieldAs(1);
        if (filterFlag) {
            innerAggFunc.add(accumulator, value.getFieldAs(0), timestamp);
        }
    }

    @Override
    public void merge(ACC_T target, ACC_T source) {
        innerAggFunc.merge(target, source);
    }

    @Override
    public void retract(ACC_T accumulator, Row value) {
        Preconditions.checkArgument(
                value.getArity() == 2, "Row to AggFuncWithFilterFlag can only have 2 columns.");
        Boolean filterFlag = value.getFieldAs(1);
        if (filterFlag) {
            innerAggFunc.retract(accumulator, value.getFieldAs(0));
        }
    }

    @Override
    public void retractAccumulator(ACC_T target, ACC_T source) {
        innerAggFunc.retractAccumulator(target, source);
    }

    @Override
    public OUT_T getResult(ACC_T accumulator) {
        return innerAggFunc.getResult(accumulator);
    }

    @Override
    public DataType getResultDatatype() {
        return innerAggFunc.getResultDatatype();
    }

    @Override
    public ACC_T createAccumulator() {
        return innerAggFunc.createAccumulator();
    }

    @Override
    public TypeInformation<ACC_T> getAccumulatorTypeInformation() {
        return innerAggFunc.getAccumulatorTypeInformation();
    }
}

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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation function that counts the average of values. */
public class AvgAggFunc<IN_T extends Number>
        implements AggFunc<
                IN_T,
                Double,
                Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>>> {
    private final CountAggFunc countAggFunc;
    private final SumAggFunc<IN_T> sumAggFunc;

    public AvgAggFunc(SumAggFunc<IN_T> sumAggFunc) {
        this.countAggFunc = new CountAggFunc();
        this.sumAggFunc = sumAggFunc;
    }

    @Override
    public void add(
            Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>> accumulator,
            IN_T value,
            long timestamp) {
        countAggFunc.add(accumulator.f0, value, timestamp);
        sumAggFunc.add(accumulator.f1, value, timestamp);
    }

    @Override
    public void merge(
            Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>> target,
            Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>> source) {
        countAggFunc.merge(target.f0, source.f0);
        sumAggFunc.merge(target.f1, source.f1);
    }

    @Override
    public void retract(
            Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>> accumulator,
            IN_T value) {
        countAggFunc.retract(accumulator.f0, value);
        sumAggFunc.retract(accumulator.f1, value);
    }

    @Override
    public void retractAccumulator(
            Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>> target,
            Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>> source) {
        countAggFunc.retractAccumulator(target.f0, source.f0);
        sumAggFunc.retractAccumulator(target.f1, source.f1);
    }

    @Override
    public Double getResult(
            Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>> accumulator) {
        long count = countAggFunc.getResult(accumulator.f0);
        if (count == 0) {
            return null;
        } else {
            return sumAggFunc.getResult(accumulator.f1).doubleValue() / count;
        }
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.DOUBLE();
    }

    @Override
    public TypeInformation<Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>>>
            getAccumulatorTypeInformation() {
        return Types.TUPLE(
                countAggFunc.getAccumulatorTypeInformation(),
                sumAggFunc.getAccumulatorTypeInformation());
    }

    @Override
    public Tuple2<CountAggFunc.CountAccumulator, SumAggFunc.SumAccumulator<IN_T>>
            createAccumulator() {
        return Tuple2.of(countAggFunc.createAccumulator(), sumAggFunc.createAccumulator());
    }
}

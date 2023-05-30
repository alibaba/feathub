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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation functions that calculates the sum of values. */
public abstract class SumAggFunc<IN_T>
        implements AggFunc<IN_T, IN_T, SumAggFunc.SumAccumulator<IN_T>> {

    @Override
    public void merge(SumAccumulator<IN_T> target, SumAccumulator<IN_T> source) {
        add(target, source.value, -1L);
    }

    @Override
    public void retractAccumulator(
            SumAggFunc.SumAccumulator<IN_T> target, SumAggFunc.SumAccumulator<IN_T> source) {
        retract(target, source.value);
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(SumAccumulator.class);
    }

    /** Accumulator for sum aggregation functions. */
    public static class SumAccumulator<T> {
        public T value;
    }

    /** Aggregation functions that calculates the sum of Integer values. */
    public static class IntSumAggFunc extends SumAggFunc<Integer> {

        @Override
        public void add(SumAccumulator<Integer> accumulator, Integer value, long timestamp) {
            accumulator.value += value;
        }

        @Override
        public void retract(SumAccumulator<Integer> accumulator, Integer value) {
            accumulator.value -= value;
        }

        @Override
        public Integer getResult(SumAccumulator<Integer> accumulator) {
            return accumulator.value;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.INT();
        }

        @Override
        public SumAccumulator<Integer> createAccumulator() {
            SumAccumulator<Integer> accumulator = new SumAccumulator<>();
            accumulator.value = 0;
            return accumulator;
        }
    }

    /** Aggregation functions that calculates the sum of Long values. */
    public static class LongSumAggFunc extends SumAggFunc<Long> {
        @Override
        public void add(SumAccumulator<Long> accumulator, Long value, long timestamp) {
            accumulator.value += value;
        }

        @Override
        public void retract(SumAccumulator<Long> accumulator, Long value) {
            accumulator.value -= value;
        }

        @Override
        public Long getResult(SumAccumulator<Long> accumulator) {
            return accumulator.value;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.BIGINT();
        }

        @Override
        public SumAccumulator<Long> createAccumulator() {
            SumAccumulator<Long> accumulator = new SumAccumulator<>();
            accumulator.value = 0L;
            return accumulator;
        }
    }

    /** Aggregation functions that calculates the sum of Float values. */
    public static class FloatSumAggFunc extends SumAggFunc<Float> {
        @Override
        public void add(SumAccumulator<Float> accumulator, Float value, long timestamp) {
            accumulator.value += value;
        }

        @Override
        public void retract(SumAccumulator<Float> accumulator, Float value) {
            accumulator.value -= value;
        }

        @Override
        public Float getResult(SumAccumulator<Float> accumulator) {
            return accumulator.value;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.FLOAT();
        }

        @Override
        public SumAccumulator<Float> createAccumulator() {
            SumAccumulator<Float> accumulator = new SumAccumulator<>();
            accumulator.value = 0.0f;
            return accumulator;
        }
    }

    /** Aggregation functions that calculates the sum of Double values. */
    public static class DoubleSumAggFunc extends SumAggFunc<Double> {
        @Override
        public void add(SumAccumulator<Double> accumulator, Double value, long timestamp) {
            accumulator.value += value;
        }

        @Override
        public void retract(SumAccumulator<Double> accumulator, Double value) {
            accumulator.value -= value;
        }

        @Override
        public Double getResult(SumAccumulator<Double> accumulator) {
            return accumulator.value;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.DOUBLE();
        }

        @Override
        public SumAccumulator<Double> createAccumulator() {
            SumAccumulator<Double> accumulator = new SumAccumulator<>();
            accumulator.value = 0.0;
            return accumulator;
        }
    }
}

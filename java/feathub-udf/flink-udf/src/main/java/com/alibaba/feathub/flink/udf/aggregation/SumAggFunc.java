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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation functions that calculates the sum of values. */
public abstract class SumAggFunc<IN_T, ACC_T> implements AggFunc<IN_T, IN_T, ACC_T> {

    /** Aggregation functions that calculates the sum of Integer values. */
    public static class IntSumAggFunc extends SumAggFunc<Integer, IntSumAggFunc.IntSumAccumulator> {

        @Override
        public void add(IntSumAccumulator accumulator, Integer value, long timestamp) {
            accumulator.agg += value;
        }

        @Override
        public void retract(IntSumAccumulator accumulator, Integer value) {
            accumulator.agg -= value;
        }

        @Override
        public Integer getResult(IntSumAccumulator accumulator) {
            return accumulator.agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.INT();
        }

        @Override
        public IntSumAccumulator createAccumulator() {
            return new IntSumAccumulator();
        }

        @Override
        public TypeInformation<IntSumAccumulator> getAccumulatorTypeInformation() {
            return Types.POJO(IntSumAccumulator.class);
        }

        /** Accumulator for {@link IntSumAggFunc}. */
        public static class IntSumAccumulator {
            public int agg = 0;
        }
    }

    /** Aggregation functions that calculates the sum of Long values. */
    public static class LongSumAggFunc extends SumAggFunc<Long, LongSumAggFunc.LongSumAccumulator> {
        @Override
        public void add(LongSumAccumulator accumulator, Long value, long timestamp) {
            accumulator.agg += value;
        }

        @Override
        public void retract(LongSumAccumulator accumulator, Long value) {
            accumulator.agg -= value;
        }

        @Override
        public Long getResult(LongSumAccumulator accumulator) {
            return accumulator.agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.BIGINT();
        }

        @Override
        public LongSumAccumulator createAccumulator() {
            return new LongSumAccumulator();
        }

        @Override
        public TypeInformation<LongSumAccumulator> getAccumulatorTypeInformation() {
            return Types.POJO(LongSumAccumulator.class);
        }

        /** Accumulator for {@link LongSumAggFunc}. */
        public static class LongSumAccumulator {
            public long agg = 0L;
        }
    }

    /** Aggregation functions that calculates the sum of Float values. */
    public static class FloatSumAggFunc
            extends SumAggFunc<Float, FloatSumAggFunc.FloatSumAccumulator> {
        @Override
        public void add(FloatSumAccumulator accumulator, Float value, long timestamp) {
            accumulator.agg += value;
        }

        @Override
        public void retract(FloatSumAccumulator accumulator, Float value) {
            accumulator.agg -= value;
        }

        @Override
        public Float getResult(FloatSumAccumulator accumulator) {
            return accumulator.agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.FLOAT();
        }

        @Override
        public FloatSumAccumulator createAccumulator() {
            return new FloatSumAccumulator();
        }

        @Override
        public TypeInformation<FloatSumAccumulator> getAccumulatorTypeInformation() {
            return Types.POJO(FloatSumAccumulator.class);
        }

        /** Accumulator for {@link FloatSumAggFunc}. */
        public static class FloatSumAccumulator {
            public float agg = 0.0f;
        }
    }

    /** Aggregation functions that calculates the sum of Double values. */
    public static class DoubleSumAggFunc
            extends SumAggFunc<Double, DoubleSumAggFunc.DoubleSumAccumulator> {
        @Override
        public void add(DoubleSumAccumulator accumulator, Double value, long timestamp) {
            accumulator.agg += value;
        }

        @Override
        public void retract(DoubleSumAccumulator accumulator, Double value) {
            accumulator.agg -= value;
        }

        @Override
        public Double getResult(DoubleSumAccumulator accumulator) {
            return accumulator.agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.DOUBLE();
        }

        @Override
        public DoubleSumAccumulator createAccumulator() {
            return new DoubleSumAccumulator();
        }

        @Override
        public TypeInformation<DoubleSumAccumulator> getAccumulatorTypeInformation() {
            return Types.POJO(DoubleSumAccumulator.class);
        }

        /** Accumulator for {@link DoubleSumAggFunc}. */
        public static class DoubleSumAccumulator {
            public double agg = 0.0;
        }
    }
}

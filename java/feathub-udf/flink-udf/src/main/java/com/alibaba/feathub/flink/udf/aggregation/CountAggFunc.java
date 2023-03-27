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

/** Aggregation function that counts the number of values. */
public class CountAggFunc implements AggFunc<Object, Long, CountAggFunc.CountAccumulator> {

    @Override
    public void add(CountAccumulator accumulator, Object value, long timestamp) {
        accumulator.cnt += 1;
    }

    @Override
    public void merge(CountAccumulator target, CountAccumulator source) {
        target.cnt += source.cnt;
    }

    @Override
    public void retract(CountAccumulator accumulator, Object value) {
        accumulator.cnt -= 1;
    }

    @Override
    public void retractAccumulator(CountAccumulator target, CountAccumulator source) {
        target.cnt -= source.cnt;
    }

    @Override
    public Long getResult(CountAccumulator accumulator) {
        return accumulator.cnt;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.BIGINT();
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator();
    }

    @Override
    public TypeInformation<CountAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(CountAccumulator.class);
    }

    /** Accumulator of {@link CountAggFunc}. */
    public static class CountAccumulator {
        public Long cnt = 0L;
    }
}

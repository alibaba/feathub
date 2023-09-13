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

/**
 * Aggregation function decorator that only aggregates up to `limit` number of most recent records.
 * The aggregation function assume no retraction so that we only need to keep up to `limit` number
 * of values in the record.
 *
 * <p>The retract methods cannot be invoked. Otherwise, exception will be thrown.
 */
public class AggFuncWithLimitWithoutRetract<IN_T, OUT_T, ACC_T>
        extends AggFuncWithLimit<IN_T, OUT_T, ACC_T> {

    public AggFuncWithLimitWithoutRetract(AggFunc<IN_T, OUT_T, ACC_T> aggFunc, long limit) {
        super(aggFunc, limit);
    }

    @Override
    public void add(RawDataAccumulator<IN_T> acc, IN_T value, long timestamp) {
        super.add(acc, value, timestamp);

        while (acc.rawDataList.size() > limit) {
            acc.rawDataList.removeFirst();
        }
    }

    @Override
    public void merge(RawDataAccumulator<IN_T> target, RawDataAccumulator<IN_T> source) {
        super.merge(target, source);

        while (target.rawDataList.size() > limit) {
            target.rawDataList.removeFirst();
        }
    }

    @Override
    public void retractAccumulator(
            RawDataAccumulator<IN_T> target, RawDataAccumulator<IN_T> source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void retract(RawDataAccumulator<IN_T> accumulator, IN_T value) {
        throw new UnsupportedOperationException();
    }
}

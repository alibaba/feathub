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

import java.util.Iterator;
import java.util.LinkedList;

import static com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils.insertIntoSortedList;
import static com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils.mergeSortedLists;

/**
 * Abstract aggregation function that collects raw data and timestamps until getResult is invoked.
 */
public abstract class RawDataAccumulatingAggFunc<IN_T, OUT_T>
        implements AggFunc<IN_T, OUT_T, RawDataAccumulatingAggFunc.RawDataAccumulator<IN_T>> {

    @Override
    public void add(RawDataAccumulator<IN_T> acc, IN_T value, long timestamp) {
        if (acc.rawDataList.isEmpty() || timestamp >= acc.rawDataList.getLast().f1) {
            acc.rawDataList.add(Tuple2.of(value, timestamp));
            return;
        }

        insertIntoSortedList(
                acc.rawDataList, Tuple2.of(value, timestamp), (o1, o2) -> (int) (o1.f1 - o2.f1));
    }

    @Override
    public void merge(RawDataAccumulator<IN_T> target, RawDataAccumulator<IN_T> source) {
        if (source.rawDataList.isEmpty()) {
            return;
        }

        if (target.rawDataList.isEmpty()) {
            target.rawDataList.addAll(source.rawDataList);
            return;
        }

        target.rawDataList =
                mergeSortedLists(
                        target.rawDataList, source.rawDataList, (o1, o2) -> (int) (o1.f1 - o2.f1));
    }

    @Override
    public void retract(RawDataAccumulator<IN_T> accumulator, IN_T value) {
        final Iterator<Tuple2<IN_T, Long>> iter = accumulator.rawDataList.iterator();
        boolean removed = false;
        while (iter.hasNext()) {
            final IN_T v = iter.next().f0;
            if ((v == null && value == null) || (v != null && v.equals(value))) {
                iter.remove();
                removed = true;
                break;
            }
        }

        if (!removed) {
            throw new RuntimeException(
                    String.format("The value %s to retract is not in the list.", value));
        }
    }

    @Override
    public void retractAccumulator(
            RawDataAccumulator<IN_T> target, RawDataAccumulator<IN_T> source) {
        for (Tuple2<IN_T, Long> value : source.rawDataList) {
            retract(target, value.f0);
        }
    }

    @Override
    public RawDataAccumulator<IN_T> createAccumulator() {
        return new RawDataAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(RawDataAccumulator.class);
    }

    /** Accumulator that collects raw data and their timestamps. */
    public static class RawDataAccumulator<T> {
        public LinkedList<Tuple2<T, Long>> rawDataList = new LinkedList<>();
    }
}

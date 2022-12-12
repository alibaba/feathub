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

import org.apache.flink.table.types.DataType;

import java.io.Serializable;

// TODO: Update AbstractTimeWindowedAggFunc to reuse the implementations of AggFunc.
/**
 * Interface of aggregation function. The aggregation function can aggregate any number of records
 * with its timestamp and get the aggregation result. It also has a reset method to reset the
 * aggregation function to its initial state.
 */
public interface AggFunc<IN_T, OUT_T> extends Serializable {

    /** Reset the aggregation function. */
    void reset();

    /**
     * Aggregate the value with the timestamp.
     *
     * @param value The value.
     * @param timestamp The timestamp of the value.
     */
    void aggregate(IN_T value, long timestamp);

    /** @return The aggregation result. */
    OUT_T getResult();

    /** @return The DataType of the aggregation result. */
    DataType getResultDatatype();
}

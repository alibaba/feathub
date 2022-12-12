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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation function that counts the number of values. */
public class CountAggFunc implements AggFunc<Object, Long> {
    private long cnt = 0;

    @Override
    public void reset() {
        cnt = 0;
    }

    @Override
    public void aggregate(Object value, long timestamp) {
        cnt += 1;
    }

    @Override
    public Long getResult() {
        return cnt;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.BIGINT();
    }
}

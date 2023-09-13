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

package com.alibaba.feathub.flink.udf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncAdaptor;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithFilterFlag;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithLimit;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithLimitWithoutRetract;

/** Utility methods to apply over windows. */
public class OverWindowUtils {

    /** Returns the {@link AggregateFunction} for over window. */
    public static <OUT_T, ACC_T> AggregateFunction<OUT_T, ACC_T> getAggregateFunction(
            String aggFuncName,
            Long limit,
            String filterExpr,
            boolean withBoundedRowTimeRange,
            DataType inDataType) {

        AggFunc<?, OUT_T, ACC_T> aggFunc =
                (AggFunc<?, OUT_T, ACC_T>) AggFuncUtils.getAggFunc(aggFuncName, inDataType, true);

        if (limit != null) {
            if (withBoundedRowTimeRange) {
                aggFunc = new AggFuncWithLimit(aggFunc, limit);
            } else {
                // Unbounded row time range over window will not retract
                aggFunc = new AggFuncWithLimitWithoutRetract(aggFunc, limit);
            }
        }

        if (filterExpr != null) {
            aggFunc = new AggFuncWithFilterFlag<>(aggFunc);
        }

        return new AggFuncAdaptor<>(aggFunc);
    }
}

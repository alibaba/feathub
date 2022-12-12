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

/** Utility of aggregation functions. */
public class AggFuncUtils {
    public static AggFunc<?, ?> getAggFunc(String aggFunc, DataType inDataType) {
        if ("SUM".equals(aggFunc)) {
            return getSumAggFunc(inDataType);
        } else if ("AVG".equals(aggFunc)) {
            return new RowAvgAggFunc(inDataType);
        } else if ("FIRST_VALUE".equals(aggFunc)) {
            return new FirstValueAggFunc<>(inDataType);
        } else if ("LAST_VALUE".equals(aggFunc)) {
            return new LastValueAggFunc<>(inDataType);
        } else if ("MAX".equals(aggFunc)) {
            return new MaxAggFunc<>(inDataType);
        } else if ("MIN".equals(aggFunc)) {
            return new MinAggFunc<>(inDataType);
        } else if ("COUNT".equals(aggFunc) || "ROW_NUMBER".equals(aggFunc)) {
            return new CountAggFunc();
        } else if ("VALUE_COUNTS".equals(aggFunc)) {
            return new MergeValueCountsAggFunc(inDataType);
        }

        throw new RuntimeException(String.format("Unsupported aggregation function %s", aggFunc));
    }

    @SuppressWarnings({"unchecked"})
    private static <IN_T> SumAggFunc<IN_T> getSumAggFunc(DataType inDataType) {
        final Class<?> inClass = inDataType.getConversionClass();
        if (inClass.equals(Integer.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.IntSumAggFunc();
        } else if (inClass.equals(Long.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.LongSumAggFunc();
        } else if (inClass.equals(Float.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.FloatSumAggFunc();
        } else if (inClass.equals(Double.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.DoubleSumAggFunc();
        }
        throw new RuntimeException(
                String.format("Unsupported type for AvgAggregationFunction %s.", inDataType));
    }
}

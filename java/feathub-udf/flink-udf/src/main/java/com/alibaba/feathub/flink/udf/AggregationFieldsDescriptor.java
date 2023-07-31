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

import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithFilterFlag;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithLimit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** The descriptor of aggregation fields in the window operator. */
public class AggregationFieldsDescriptor implements Serializable {
    private final List<AggregationFieldDescriptor> aggregationFieldDescriptors;

    private final Map<String, Integer> fieldNameToIdx = new HashMap<>();

    private Long maxWindowSizeMs;

    AggregationFieldsDescriptor(List<AggregationFieldDescriptor> aggregationFieldDescriptors) {
        this.aggregationFieldDescriptors = aggregationFieldDescriptors;
        int idx = 0;
        for (AggregationFieldDescriptor descriptor : this.aggregationFieldDescriptors) {
            fieldNameToIdx.put(descriptor.fieldName, idx);
            idx += 1;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<AggregationFieldDescriptor> getAggFieldDescriptors() {
        return aggregationFieldDescriptors;
    }

    public long getMaxWindowSizeMs() {
        if (maxWindowSizeMs == null) {
            maxWindowSizeMs =
                    aggregationFieldDescriptors.stream()
                            .mapToLong(descriptor -> descriptor.windowSizeMs)
                            .max()
                            .orElseThrow(
                                    () -> new RuntimeException("Fail to get max window size."));
        }
        return maxWindowSizeMs;
    }

    /** Get the index of the given {@link AggregationFieldDescriptor} in the list. */
    public int getAggFieldIdx(AggregationFieldDescriptor descriptor) {
        if (!fieldNameToIdx.containsKey(descriptor.fieldName)) {
            throw new RuntimeException(
                    String.format("The given fieldName %s doesn't exist.", descriptor.fieldName));
        }
        return fieldNameToIdx.get(descriptor.fieldName);
    }

    /** Builder for {@link AggregationFieldsDescriptor}. */
    public static class Builder {
        private final List<AggregationFieldDescriptor> aggregationFieldDescriptors;

        private Builder() {
            aggregationFieldDescriptors = new LinkedList<>();
        }

        public Builder addField(
                String fieldName,
                DataType inDataType,
                DataType outDataType,
                Long windowSizeMs,
                Long limit,
                String filterExpr,
                String aggFunc) {
            aggregationFieldDescriptors.add(
                    new AggregationFieldDescriptor(
                            fieldName,
                            inDataType,
                            outDataType,
                            windowSizeMs,
                            limit,
                            filterExpr,
                            aggFunc));
            return this;
        }

        public AggregationFieldsDescriptor build() {
            return new AggregationFieldsDescriptor(aggregationFieldDescriptors);
        }
    }

    /** The descriptor of an aggregation field. */
    public static class AggregationFieldDescriptor implements Serializable {
        public final String fieldName;
        public final DataType dataType;
        public final Long windowSizeMs;
        public final Long limit;
        public final String filterExpr;
        public final AggFunc<Object, ?, Object> aggFunc;
        public final AggFunc<Object, ?, Object> aggFuncWithoutRetract;

        @SuppressWarnings({"unchecked"})
        public AggregationFieldDescriptor(
                String fieldName,
                DataType inDataType,
                DataType outDataType,
                Long windowSizeMs,
                Long limit,
                String filterExpr,
                String aggFuncName) {
            this.fieldName = fieldName;
            this.dataType = outDataType;
            this.windowSizeMs = windowSizeMs;
            this.limit = limit;
            this.filterExpr = filterExpr;

            AggFunc tmpAggFunc = AggFuncUtils.getAggFunc(aggFuncName, inDataType, true);
            AggFunc tmpAggFuncWithoutRetract =
                    AggFuncUtils.getAggFunc(aggFuncName, inDataType, false);

            if (limit != null) {
                tmpAggFunc = new AggFuncWithLimit<>(tmpAggFuncWithoutRetract, limit);
                tmpAggFuncWithoutRetract = tmpAggFunc;
            }

            if (filterExpr != null) {
                tmpAggFuncWithoutRetract = new AggFuncWithFilterFlag(tmpAggFuncWithoutRetract);
            }

            this.aggFunc = tmpAggFunc;
            this.aggFuncWithoutRetract = tmpAggFuncWithoutRetract;
        }
    }
}

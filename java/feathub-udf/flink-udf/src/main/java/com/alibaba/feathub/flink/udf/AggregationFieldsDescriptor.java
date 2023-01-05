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

package com.alibaba.feathub.flink.udf;

import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;
import com.alibaba.feathub.flink.udf.aggregation.AggFuncUtils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/** The descriptor of aggregation fields in the window operator. */
public class AggregationFieldsDescriptor implements Serializable {
    private final List<AggregationFieldDescriptor> aggregationFieldDescriptors;

    private Long maxWindowSizeMs;

    private AggregationFieldsDescriptor(
            List<AggregationFieldDescriptor> aggregationFieldDescriptors) {
        this.aggregationFieldDescriptors = aggregationFieldDescriptors;
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

    /** Builder for {@link AggregationFieldsDescriptor}. */
    public static class Builder {
        private final List<AggregationFieldDescriptor> aggregationFieldDescriptors;

        private Builder() {
            aggregationFieldDescriptors = new LinkedList<>();
        }

        public Builder addField(
                String inFieldName,
                DataType inDataType,
                String outFieldNames,
                DataType outDataType,
                Long windowSizeMs,
                String aggFunc) {
            aggregationFieldDescriptors.add(
                    new AggregationFieldDescriptor(
                            inFieldName,
                            inDataType,
                            outFieldNames,
                            outDataType,
                            windowSizeMs,
                            aggFunc));
            return this;
        }

        public AggregationFieldsDescriptor build() {
            return new AggregationFieldsDescriptor(aggregationFieldDescriptors);
        }
    }

    /** The descriptor of an aggregation field. */
    public static class AggregationFieldDescriptor implements Serializable {
        public String inFieldName;
        public String outFieldName;
        public DataType outDataType;
        public Long windowSizeMs;
        public AggFunc<Object, ?> aggFunc;

        @SuppressWarnings({"unchecked"})
        public AggregationFieldDescriptor(
                String inFieldName,
                DataType inDataType,
                String outFieldNames,
                DataType outDataType,
                Long windowSizeMs,
                String aggFunc) {
            this.inFieldName = inFieldName;
            this.outFieldName = outFieldNames;
            this.outDataType = outDataType;
            this.windowSizeMs = windowSizeMs;
            this.aggFunc = (AggFunc<Object, ?>) AggFuncUtils.getAggFunc(aggFunc, inDataType);
        }
    }
}

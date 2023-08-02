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

package com.alibaba.feathub.flink.udf.processfunction;

import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;

import java.util.Objects;

/** Common util methods for window process functions. */
class WindowUtils {

    /** Return true if the two rows have the same aggregation results. */
    static boolean hasEqualAggregationResult(
            AggregationFieldsDescriptor aggDescriptors, Row row0, Row row1) {
        if (row0 == null || row1 == null) {
            return row0 == null && row1 == null;
        }

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggDescriptors.getAggFieldDescriptors()) {
            if (!Objects.equals(
                    row0.getField(descriptor.fieldName), row1.getField(descriptor.fieldName))) {
                return false;
            }
        }

        return true;
    }
}

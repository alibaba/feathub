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

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Descriptor of a sliding window. */
public class SlidingWindowDescriptor implements Serializable {
    public final Duration stepSize;
    public final Integer limit;
    public final List<String> groupByKeys;
    public final String filterExpr;

    public SlidingWindowDescriptor(
            Duration stepSize, Integer limit, List<String> groupByKeys, String filterExpr) {
        this.stepSize = stepSize;
        this.limit = limit;
        this.groupByKeys = groupByKeys;
        this.filterExpr = filterExpr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stepSize, limit, groupByKeys, filterExpr);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SlidingWindowDescriptor)) {
            return false;
        }
        SlidingWindowDescriptor descriptor = (SlidingWindowDescriptor) obj;
        return Objects.equals(this.stepSize, descriptor.stepSize)
                && Objects.equals(this.limit, descriptor.limit)
                && Objects.equals(this.groupByKeys, descriptor.groupByKeys)
                && Objects.equals(this.filterExpr, descriptor.filterExpr);
    }
}

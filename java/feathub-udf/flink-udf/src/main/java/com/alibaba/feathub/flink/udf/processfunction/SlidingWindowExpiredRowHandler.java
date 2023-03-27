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
import org.apache.flink.util.Collector;

import java.io.Serializable;

/** Interface for handling post sliding window expired row. */
// TODO: Implement a SlidingWindowExpiredRowHandler that retract the expired row.
public interface SlidingWindowExpiredRowHandler extends Serializable {
    void handleExpiredRow(Collector<Row> out, Row expiredRow, long currentTimestamp);
}

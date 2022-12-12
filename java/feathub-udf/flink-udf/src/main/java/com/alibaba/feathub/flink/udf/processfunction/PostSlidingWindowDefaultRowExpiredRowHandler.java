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

package com.alibaba.feathub.flink.udf.processfunction;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * PostSlidingWindowDefaultRowExpiredRowHandler output a row with the default values when a row is
 * expired.
 */
public class PostSlidingWindowDefaultRowExpiredRowHandler
        implements PostSlidingWindowExpiredRowHandler {

    private final Row defaultRow;
    private final String rowTimeFieldName;
    private final String[] keyFieldNames;

    public PostSlidingWindowDefaultRowExpiredRowHandler(
            Row defaultRow, String rowTimeFieldName, String... keyFieldNames) {
        this.defaultRow = defaultRow;
        this.rowTimeFieldName = rowTimeFieldName;
        this.keyFieldNames = keyFieldNames;
    }

    @Override
    public void handleExpiredRow(Collector<Row> out, Row expiredRow, long currentTimestamp) {
        Row row = Row.copy(defaultRow);
        row.setField(rowTimeFieldName, Instant.ofEpochMilli(currentTimestamp));
        for (String keyFieldName : keyFieldNames) {
            row.setField(keyFieldName, expiredRow.getField(keyFieldName));
        }
        out.collect(row);
    }
}

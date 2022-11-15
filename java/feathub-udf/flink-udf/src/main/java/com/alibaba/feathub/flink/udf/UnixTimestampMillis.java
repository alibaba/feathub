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

import org.apache.flink.table.functions.ScalarFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/** Convert the given TIMESTAMP type value to number of milliseconds since epoch. */
public class UnixTimestampMillis extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(UnixTimestampMillis.class);
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private SimpleDateFormat formatter;

    public Long eval(String timestampStr, String zoneId) {
        return this.eval(timestampStr, DEFAULT_FORMAT, zoneId);
    }

    public Long eval(String timestampStr, String format, String zoneId) {
        // The UDF is only used by FeatHub internally. FeatHub guarantee that
        // the format and zoneId will not change across rows.
        if (formatter == null) {
            formatter = new SimpleDateFormat(format);
            formatter.setTimeZone(TimeZone.getTimeZone(zoneId));
        }
        try {
            Date date = formatter.parse(timestampStr);
            return date.getTime();
        } catch (ParseException e) {
            LOG.error(
                    String.format(
                            "Exception when parsing datetime string '%s' in format '%s'",
                            timestampStr, format),
                    e);
            return Long.MIN_VALUE;
        }
    }
}

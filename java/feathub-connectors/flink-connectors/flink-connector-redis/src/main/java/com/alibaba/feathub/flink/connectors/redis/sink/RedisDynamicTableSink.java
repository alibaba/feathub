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

package com.alibaba.feathub.flink.connectors.redis.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

/** A {@link DynamicTableSink} that writes to a Redis database. */
public class RedisDynamicTableSink implements DynamicTableSink {
    private final ReadableConfig config;

    private final ResolvedSchema schema;

    public RedisDynamicTableSink(ReadableConfig config, ResolvedSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new RedisSinkFunction(config, schema));
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(config, schema);
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}

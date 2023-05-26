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

package com.alibaba.feathub.flink.connectors.redis.lookup;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;

/** A {@link LookupTableSource} that queries from a Redis database. */
public class RedisLookupTableSource implements LookupTableSource {

    private final ReadableConfig config;

    private final ResolvedSchema schema;

    public RedisLookupTableSource(ReadableConfig config, ResolvedSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return LookupFunctionProvider.of(new RedisLookupFunction(config, schema));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisLookupTableSource(config, schema);
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}

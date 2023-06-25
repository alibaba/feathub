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

package com.alibaba.feathub.flink.connectors.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.alibaba.feathub.flink.connectors.redis.lookup.RedisLookupTableSource;
import com.alibaba.feathub.flink.connectors.redis.sink.RedisDynamicTableSink;

import java.util.HashSet;
import java.util.Set;

import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.DB_NUM;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.ENABLE_HASH_PARTIAL_UPDATE;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.HOST;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.KEY_FIELDS;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.PASSWORD;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.PORT;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.REDIS_MODE;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.USERNAME;

/** The table factory for {@link RedisLookupTableSource} and {@link RedisDynamicTableSink}. */
public class RedisDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        return new RedisLookupTableSource(
                helper.getOptions(), context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        return new RedisDynamicTableSink(
                helper.getOptions(), context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(PORT);
        options.add(DB_NUM);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REDIS_MODE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(KEY_FIELDS);
        options.add(ENABLE_HASH_PARTIAL_UPDATE);
        return options;
    }
}

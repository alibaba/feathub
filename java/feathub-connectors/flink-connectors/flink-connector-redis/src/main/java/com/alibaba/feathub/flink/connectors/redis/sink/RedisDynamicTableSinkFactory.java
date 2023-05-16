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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.DB_NUM;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.HOST;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.KEY_FIELDS;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.NAMESPACE;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.PASSWORD;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.PORT;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.REDIS_MODE;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.USERNAME;

/** {@link DynamicTableSinkFactory} for {@link RedisDynamicTableSink}. */
public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {
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
        options.add(NAMESPACE);
        options.add(KEY_FIELDS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REDIS_MODE);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }
}

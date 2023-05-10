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
import org.apache.flink.configuration.ConfigOptions;

/** Configurations used by Redis sink. */
public class RedisSinkConfigs {
    static final ConfigOption<RedisMode> REDIS_MODE =
            ConfigOptions.key("mode")
                    .enumType(RedisMode.class)
                    .defaultValue(RedisMode.STANDALONE)
                    .withDescription("The deployment mode of the Redis service to connect.");

    static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The host of the Redis instance to connect.");

    static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The port of the Redis instance to connect.");

    static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username used by the Redis authorization process.");

    static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password used by the Redis authorization process.");

    static final ConfigOption<Integer> DB_NUM =
            ConfigOptions.key("dbNum")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The No. of the Redis database to connect. Not supported in cluster mode.");

    static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key("namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A string that identifies a namespace for Redis keys. "
                                    + "Input tables with different namespaces can save "
                                    + "records with the same key into Redis without "
                                    + "overwriting each other.");

    static final ConfigOption<String> KEY_FIELD =
            ConfigOptions.key("keyField")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the key field in the input table. "
                                    + "Values in this field would be concatenated with the "
                                    + "namespace and used as keys in Redis storage");

    static final ConfigOption<String> TIMESTAMP_FIELD =
            ConfigOptions.key("timestampField")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the timestamp field in the input table. "
                                    + "The values in this field must be Long values representing "
                                    + "the milliseconds from epoch. If two records with the same "
                                    + "key and namespace but different timestamp are written out "
                                    + "through this sink, the record with larger timestamp value "
                                    + "will finally be persisted to Redis.");

    enum RedisMode {
        STANDALONE,
        MASTER_SLAVE,
        CLUSTER,
    }
}

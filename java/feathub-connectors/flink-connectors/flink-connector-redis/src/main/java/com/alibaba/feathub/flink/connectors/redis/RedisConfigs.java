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
import org.apache.flink.configuration.ConfigOptions;

/** Configurations used by Redis sink. */
public class RedisConfigs {
    public static final ConfigOption<RedisMode> REDIS_MODE =
            ConfigOptions.key("mode")
                    .enumType(RedisMode.class)
                    .defaultValue(RedisMode.STANDALONE)
                    .withDescription("The deployment mode of the Redis service to connect.");

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The host of the Redis instance to connect.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The port of the Redis instance to connect.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username used by the Redis authorization process.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password used by the Redis authorization process.");

    public static final ConfigOption<Integer> DB_NUM =
            ConfigOptions.key("dbNum")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The No. of the Redis database to connect. Not supported in cluster mode.");

    public static final ConfigOption<String> KEY_FIELDS =
            ConfigOptions.key("keyFields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A comma-separated list of field names in the input table containing "
                                    + "the key values used to derive Redis keys. Used in lookup source.");

    public static final ConfigOption<Boolean> ENABLE_HASH_PARTIAL_UPDATE =
            ConfigOptions.key("enableHashPartialUpdate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, map-typed data (or hash in Redis) would be partially updated "
                                    + "instead of completely overridden by new data.");

    /** Supported Redis deployment modes. */
    public enum RedisMode {
        STANDALONE,
        MASTER_SLAVE,
        CLUSTER,
    }
}

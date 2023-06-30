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

import org.apache.flink.configuration.ReadableConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.DB_NUM;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.HOST;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.PASSWORD;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.PORT;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.REDIS_MODE;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.USERNAME;

/** A wrapper interface for jedis clients in different deployment modes. */
public interface JedisClient {
    void del(String key);

    Map<String, String> hgetAll(final String key);

    void hmset(String key, Map<String, String> hash);

    List<String> hmget(String key, String... fields);

    void rpush(String key, String... string);

    String get(String key);

    void set(String key, String value);

    void lpush(String key, String... string);

    void lpop(String key, int count);

    void rpop(String key, int count);

    List<String> lrange(String key, int start, int stop);

    Set<String> hkeys(String key);

    void hdel(String key, String... field);

    void flush();

    void close();

    static JedisClient create(ReadableConfig config) {
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String host = config.get(HOST);
        int port = config.get(PORT);
        if (Objects.requireNonNull(config.get(REDIS_MODE)) == RedisConfigs.RedisMode.CLUSTER) {
            return new JedisClusterClient(host, port, username, password);
        }
        int dbNum = config.get(DB_NUM);
        return new JedisMasterClient(host, port, username, password, dbNum);
    }
}

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

import org.apache.flink.util.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Map;

/**
 * A subclass of {@link JedisClient} that connects to a Redis master instance. Used for Redis
 * Standalone and Master-Slave mode.
 */
public class JedisMasterClient implements JedisClient {
    private final Jedis jedis;

    private final Pipeline pipeline;

    public JedisMasterClient(String host, int port, String username, String password, int dbNum) {
        jedis = new Jedis(host, port);
        if (!StringUtils.isNullOrWhitespaceOnly(username)) {
            jedis.auth(username, password);
        } else if (!StringUtils.isNullOrWhitespaceOnly(password)) {
            jedis.auth(password);
        }
        jedis.select(dbNum);
        pipeline = jedis.pipelined();
    }

    @Override
    public void del(String key) {
        pipeline.del(key);
    }

    @Override
    public void hmset(String key, Map<String, String> hash) {
        pipeline.hmset(key, hash);
    }

    @Override
    public void rpush(String key, String... string) {
        pipeline.rpush(key, string);
    }

    @Override
    public void set(String key, String value) {
        pipeline.set(key, value);
    }

    @Override
    public void flush() {
        pipeline.sync();
    }

    @Override
    public void close() {
        pipeline.close();
        jedis.close();
    }
}

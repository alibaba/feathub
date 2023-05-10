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

import java.util.List;
import java.util.Map;

/**
 * A subclass of {@link JedisClient} that connects to a Redis master instance. Used for Redis
 * Standalone and Master-Slave mode.
 */
public class JedisMasterClient implements JedisClient {
    private final Jedis jedis;

    public JedisMasterClient(String host, int port, String username, String password, int dbNum) {
        jedis = new Jedis(host, port);
        if (!StringUtils.isNullOrWhitespaceOnly(username)) {
            jedis.auth(username, password);
        } else if (!StringUtils.isNullOrWhitespaceOnly(password)) {
            jedis.auth(password);
        }
        jedis.select(dbNum);
    }

    @Override
    public long hset(byte[] key, Map<byte[], byte[]> hash) {
        return this.jedis.hset(key, hash);
    }

    @Override
    public byte[] scriptLoad(byte[] script) {
        return this.jedis.scriptLoad(script);
    }

    @Override
    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        return this.jedis.evalsha(sha1, keys, args);
    }

    @Override
    public void close() {
        jedis.close();
    }
}

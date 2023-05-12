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

import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;

import java.util.Collections;
import java.util.Map;

/**
 * A subclass of {@link JedisClient} that connects to a Redis cluster. Used for Redis Cluster mode.
 */
public class JedisClusterClient implements JedisClient {
    private final ClusterPipeline pipeline;

    public JedisClusterClient(String host, int port, String username, String password) {
        this.pipeline =
                new ClusterPipeline(
                        Collections.singleton(new HostAndPort(host, port)),
                        DefaultJedisClientConfig.builder()
                                .user(username)
                                .password(password)
                                .build());
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
    }
}

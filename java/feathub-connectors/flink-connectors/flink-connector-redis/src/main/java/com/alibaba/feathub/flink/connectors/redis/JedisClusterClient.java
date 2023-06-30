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

import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A subclass of {@link JedisClient} that connects to a Redis cluster. Used for Redis Cluster mode.
 */
public class JedisClusterClient implements JedisClient {
    private final ClusterPipeline pipeline;

    private final UnifiedJedis jedis;

    public JedisClusterClient(String host, int port, String username, String password) {
        this.pipeline =
                new ClusterPipeline(
                        Collections.singleton(new HostAndPort(host, port)),
                        DefaultJedisClientConfig.builder()
                                .user(username)
                                .password(password)
                                .build());

        this.jedis =
                new JedisCluster(
                        Collections.singleton(new HostAndPort(host, port)), username, password);
    }

    @Override
    public void del(String key) {
        pipeline.del(key);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return jedis.hgetAll(key);
    }

    @Override
    public void hmset(String key, Map<String, String> hash) {
        pipeline.hmset(key, hash);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return jedis.hmget(key, fields);
    }

    @Override
    public void rpush(String key, String... string) {
        pipeline.rpush(key, string);
    }

    @Override
    public String get(String key) {
        return jedis.get(key);
    }

    @Override
    public void set(String key, String value) {
        pipeline.set(key, value);
    }

    @Override
    public void lpush(String key, String... string) {
        pipeline.lpush(key, string);
    }

    @Override
    public void lpop(String key, int count) {
        pipeline.lpop(key, count);
    }

    @Override
    public void rpop(String key, int count) {
        pipeline.rpop(key, count);
    }

    @Override
    public List<String> lrange(String key, int start, int stop) {
        return jedis.lrange(key, start, stop);
    }

    @Override
    public Set<String> hkeys(String key) {
        return jedis.hkeys(key);
    }

    @Override
    public void hdel(String key, String... field) {
        pipeline.hdel(key, field);
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

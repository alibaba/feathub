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

import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A subclass of {@link JedisClient} that connects to a Redis cluster. Used for Redis Cluster mode.
 */
public class JedisClusterClient implements JedisClient {
    private static final byte[] SAMPLE_KEY = "SAMPLE_KEY".getBytes();

    private final JedisCluster cluster;

    private final BidiMap scriptShaMap;

    public JedisClusterClient(String host, int port, String username, String password) {
        this.cluster =
                new JedisCluster(
                        Collections.singleton(new HostAndPort(host, port)), username, password);
        this.scriptShaMap = new DualHashBidiMap();
    }

    @Override
    public long hset(byte[] key, Map<byte[], byte[]> hash) {
        return this.cluster.hset(key, hash);
    }

    @Override
    public byte[] scriptLoad(byte[] script) {
        if (scriptShaMap.containsValue(script)) {
            return (byte[]) scriptShaMap.getKey(script);
        }
        // Loads the script on an arbitrary cluster node and cache it locally
        // because Redis does not support loading script on all nodes in one time.
        byte[] sha = this.cluster.scriptLoad(script, SAMPLE_KEY);
        scriptShaMap.put(sha, script);
        return sha;
    }

    @Override
    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        try {
            return this.cluster.evalsha(sha1, keys, args);
        } catch (JedisNoScriptException e) {
            // Load the cached script onto the relevant cluster node dynamically.
            byte[] script = (byte[]) scriptShaMap.get(sha1);
            for (byte[] key : keys) {
                byte[] sha = this.cluster.scriptLoad(script, key);
                Preconditions.checkState(Arrays.equals(sha1, sha));
            }
            return this.cluster.evalsha(sha1, keys, args);
        }
    }

    @Override
    public void close() {
        this.cluster.close();
    }
}

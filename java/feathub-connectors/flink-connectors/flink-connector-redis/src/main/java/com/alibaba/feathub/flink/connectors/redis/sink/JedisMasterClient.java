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

import redis.clients.jedis.CommandObject;
import redis.clients.jedis.CommandObjects;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A subclass of {@link JedisClient} that connects to a Redis master instance. Used for Redis
 * Standalone and Master-Slave mode.
 */
public class JedisMasterClient implements JedisClient {
    private final Jedis jedis;

    private final CommandObjects commandObjects;

    private final List<CommandObject<?>> commands;

    public JedisMasterClient(String host, int port, String username, String password, int dbNum) {
        jedis = new Jedis(host, port);
        if (!StringUtils.isNullOrWhitespaceOnly(username)) {
            jedis.auth(username, password);
        } else if (!StringUtils.isNullOrWhitespaceOnly(password)) {
            jedis.auth(password);
        }
        jedis.select(dbNum);
        commandObjects = new CommandObjects();
        commands = new ArrayList<>();
    }

    @Override
    public void del(String key) {
        commands.add(commandObjects.del(key));
    }

    @Override
    public void hmset(String key, Map<String, String> hash) {
        commands.add(commandObjects.hmset(key, hash));
    }

    @Override
    public void rpush(String key, String... string) {
        commands.add(commandObjects.rpush(key, string));
    }

    @Override
    public void set(String key, String value) {
        commands.add(commandObjects.set(key, value));
    }

    @Override
    public void lpush(String key, String... string) {
        commands.add(commandObjects.lpush(key, string));
    }

    @Override
    public void lpop(String key, int count) {
        commands.add(commandObjects.lpop(key, count));
    }

    @Override
    public void rpop(String key, int count) {
        commands.add(commandObjects.rpop(key, count));
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
        commands.add(commandObjects.hdel(key, field));
    }

    @Override
    public void flush() {
        Pipeline pipeline = jedis.pipelined();
        for (CommandObject<?> command : commands) {
            pipeline.appendCommand(command);
        }
        pipeline.sync();
        pipeline.close();
        commands.clear();
    }

    @Override
    public void close() {
        jedis.close();
    }
}

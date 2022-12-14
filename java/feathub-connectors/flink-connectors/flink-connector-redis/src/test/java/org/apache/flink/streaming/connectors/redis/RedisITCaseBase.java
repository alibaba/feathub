/*
 * Copyright 2022 The Feathub Authors
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

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.NetUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.embedded.RedisServer;

import java.io.IOException;

import static org.apache.flink.util.NetUtils.getAvailablePort;

/** Base class for Redis connector tests that use a standalone Redis cluster. */
public abstract class RedisITCaseBase extends AbstractTestBase {
    protected static final String REDIS_HOST = "127.0.0.1";

    protected static NetUtils.Port redisPort;

    private static RedisServer redisServer;

    @BeforeClass
    public static void createRedisServer() throws IOException {
        redisPort = getAvailablePort();
        redisServer = new RedisServer(redisPort.getPort());
        redisServer.start();
    }

    @AfterClass
    public static void stopRedisServer() throws Exception {
        redisServer.stop();
        redisPort.close();
    }
}

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

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests redis sink. */
public class RedisSinkITCase extends RedisITCaseBase {

    private static final int NUM_ELEMENTS = 20;

    private StreamExecutionEnvironment env;

    private StreamTableEnvironment tEnv;

    private Jedis jedis;

    @Before
    public void setUp() {
        jedis = new Jedis(REDIS_HOST, redisPort.getPort());
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    private void buildAndExecute(DataStream<Row> stream, Map<String, Object> configs)
            throws Exception {
        Table table = tEnv.fromDataStream(stream);

        TableDescriptor.Builder builder =
                TableDescriptor.forConnector("redis")
                        .schema(
                                Schema.newBuilder()
                                        .fromResolvedSchema(table.getResolvedSchema())
                                        .build());

        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            builder = builder.option(entry.getKey(), String.valueOf(entry.getValue()));
        }

        tEnv.createTemporaryTable("redis_sink", builder.build());

        table.executeInsert("redis_sink").await(30, TimeUnit.SECONDS);
    }

    private void verifyOutputResult() {
        assertThat(jedis.keys("*")).hasSize(NUM_ELEMENTS);

        for (int i = 1; i <= NUM_ELEMENTS; i++) {
            assertThat(jedis.get("test_namespace:" + i + ":f1")).isEqualTo(Integer.toString(i));
        }
    }

    @Test
    public void testWriteToRedis() throws Exception {
        DataStream<Row> stream =
                env.fromSequence(1, NUM_ELEMENTS)
                        .map(
                                x -> Row.of(x.toString().getBytes(), x.toString().getBytes()),
                                new RowTypeInfo(
                                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                                        Types.PRIMITIVE_ARRAY(Types.BYTE)));

        Map<String, Object> configs = new HashMap<>();
        configs.put("host", REDIS_HOST);
        configs.put("port", redisPort.getPort());
        configs.put("namespace", "test_namespace");
        configs.put("keyFields", "f0");
        configs.put("dbNum", 0);

        buildAndExecute(stream, configs);
        verifyOutputResult();
    }

    @Test
    public void testChangeDBNum() throws Exception {
        int dbNum = 2;

        DataStream<Row> stream =
                env.fromSequence(1, NUM_ELEMENTS)
                        .map(
                                x -> Row.of(x.toString().getBytes(), x.toString().getBytes()),
                                new RowTypeInfo(
                                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                                        Types.PRIMITIVE_ARRAY(Types.BYTE)));
        Map<String, Object> configs = new HashMap<>();
        configs.put("host", REDIS_HOST);
        configs.put("port", redisPort.getPort());
        configs.put("namespace", "test_namespace");
        configs.put("keyFields", "f0");
        configs.put("dbNum", dbNum);

        buildAndExecute(stream, configs);

        assertThat(jedis.keys("*")).isEmpty();
        jedis.select(dbNum);
        verifyOutputResult();
    }

    @Test
    public void testAllKeyFields() {
        DataStream<Row> stream = env.fromSequence(1, NUM_ELEMENTS).map(Row::of);

        Map<String, Object> configs = new HashMap<>();
        configs.put("host", REDIS_HOST);
        configs.put("port", redisPort.getPort());
        configs.put("namespace", "test_namespace");
        configs.put("keyFields", "f0");
        configs.put("dbNum", 0);

        assertThatThrownBy(() -> buildAndExecute(stream, configs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("There should be at least one value field.");
    }

    @After
    public void tearDown() {
        for (String key : jedis.keys("*")) {
            jedis.del(key);
        }

        if (jedis != null) {
            jedis.close();
        }
    }
}

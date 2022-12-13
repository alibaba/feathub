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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisCluster;
import redis.embedded.util.JedisUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests redis sink on a Redis cluster with cluster mode and sentinels enabled. */
public class RedisSentinelClusterITCase extends AbstractTestBase {
    private static final int NUM_ELEMENTS = 20;

    private static final String REDIS_MASTER = "master";

    private static RedisCluster cluster;

    private StreamExecutionEnvironment env;

    private StreamTableEnvironment tEnv;

    private Jedis jedis;

    @BeforeClass
    public static void setUpCluster() {
        cluster =
                RedisCluster.builder()
                        .ephemeralServers()
                        .quorumSize(1)
                        .replicationGroup(REDIS_MASTER, 2)
                        .build();
        cluster.start();
    }

    @Before
    public void setUp() {
        List<String> url = getMasterUrl(cluster);
        jedis = new Jedis(url.get(0), Integer.parseInt(url.get(1)));
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testWriteToRedis() throws Exception {
        DataStream<Row> stream =
                env.fromSequence(1, NUM_ELEMENTS)
                        .map(
                                x ->
                                        Row.of(
                                                x.toString().getBytes(StandardCharsets.UTF_8),
                                                x.toString().getBytes(StandardCharsets.UTF_8)),
                                new RowTypeInfo(
                                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                                        Types.PRIMITIVE_ARRAY(Types.BYTE)));
        Table table = tEnv.fromDataStream(stream);

        List<String> url = getMasterUrl(cluster);
        String host = url.get(0);
        String port = url.get(1);

        TableDescriptor descriptor =
                TableDescriptor.forConnector("redis")
                        .schema(table.getSchema().toSchema())
                        .option("host", host)
                        .option("port", port)
                        .option("dbNum", "0")
                        .option("namespace", "test_namespace")
                        .option("keyField", "f0")
                        .build();

        tEnv.createTemporaryTable("redis_sink", descriptor);

        table.executeInsert("redis_sink").await(30, TimeUnit.SECONDS);

        assertThat(jedis.keys("*")).hasSize(NUM_ELEMENTS);

        for (int i = 1; i <= NUM_ELEMENTS; i++) {
            Map<byte[], byte[]> originalResult =
                    jedis.hgetAll(("test_namespace:" + i).getBytes(StandardCharsets.UTF_8));
            Map<String, String> convertedResult = new HashMap<>();
            for (Map.Entry<byte[], byte[]> entry : originalResult.entrySet()) {
                convertedResult.put(
                        new String(entry.getKey(), StandardCharsets.UTF_8),
                        new String(entry.getValue(), StandardCharsets.UTF_8));
            }
            assertThat(convertedResult)
                    .containsOnlyKeys(new String(ByteBuffer.allocate(4).putInt(1).array()))
                    .containsValue(String.valueOf(i));
        }

        for (String key : jedis.keys("*")) {
            jedis.del(key);
        }
    }

    @AfterClass
    public static void tearDownCluster() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    private static List<String> getMasterUrl(RedisCluster cluster) {
        String sentinelUrl = JedisUtil.sentinelHosts(cluster).iterator().next();
        Jedis jedis = new Jedis("https://" + sentinelUrl);
        return jedis.sentinelGetMasterAddrByName(REDIS_MASTER);
    }
}

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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.collections.IteratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests redis lookup source. */
public class RedisLookupSourceITCase extends RedisITCaseBase {

    private StreamTableEnvironment tEnv;

    private Schema schema;

    private Jedis jedis;

    @Before
    public void setUp() {
        jedis = new Jedis(REDIS_HOST, redisPort.getPort());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> stream = env.fromElements(Row.of("0"), Row.of("1"), Row.of("2"));

        Schema scanSourceSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .columnByExpression("proctime", "PROCTIME()")
                        .build();
        tEnv.createTemporaryView("scan_source", stream, scanSourceSchema);

        schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.STRING())
                        .build();

        jedis.set("test_namespace:0:f1", "a");
        jedis.set("test_namespace:1:f1", "b");
    }

    @Test
    public void testRedisLookup() {
        TableDescriptor descriptor =
                TableDescriptor.forConnector("redis")
                        .schema(schema)
                        .option("host", REDIS_HOST)
                        .option("port", Integer.toString(redisPort.getPort()))
                        .option("dbNum", "0")
                        .option("namespace", "test_namespace")
                        .option("keyFields", "f0")
                        .build();

        tEnv.createTemporaryTable("lookup_source", descriptor);

        Row[] expectedResult =
                new Row[] {Row.of("0", "0", "a"), Row.of("1", "1", "b"), Row.of("2", "2", null)};

        List<Row> actualResult =
                IteratorUtils.toList(
                        tEnv.executeSql(
                                        "SELECT scan_source.f0, lookup_source.f0 AS f0_2, lookup_source.f1 "
                                                + "FROM scan_source LEFT JOIN lookup_source "
                                                + "FOR SYSTEM_TIME AS OF scan_source.proctime "
                                                + "ON scan_source.f0 = lookup_source.f0;")
                                .collect());
        assertThat(actualResult).containsExactly(expectedResult);
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

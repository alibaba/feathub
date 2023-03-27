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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BoundedKafkaDynamicSource}. */
public class BoundedKafkaTableITCase extends KafkaTableTestBase {

    @Test
    public void testBoundedKafkaSource() throws Exception {
        final String topic = "test-topic";
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "create table kafka (\n"
                                + "  id INTEGER,\n"
                                + "  val DOUBLE\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'format' = 'csv'\n"
                                + ")",
                        BoundedKafkaDynamicTableFactory.IDENTIFIER, topic, bootstraps, groupId);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "SELECT id, val \n"
                        + "FROM (VALUES (1, 2.02), (1, 1.11), (1, 50), (1, 3.1), (2, 5.33), (2, 0.0))\n"
                        + "  AS orders (id, val)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        String query = "SELECT * FROM kafka\n";

        DataStream<Row> result = tEnv.toDataStream(tEnv.sqlQuery(query));
        final List<Row> rows = CollectionUtil.iteratorToList(result.executeAndCollect());
        assertThat(rows).hasSize(6);

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }
}

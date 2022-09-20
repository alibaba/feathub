#  Copyright 2022 The Feathub Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import os
import unittest
from datetime import datetime
from unittest.mock import patch, Mock

from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    TableDescriptor as NativeFlinkTableDescriptor,
    DataTypes,
)

from feathub.common.types import Int64, String
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.processors.flink.table_builder.flink_table_builder_constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.table_builder.source_sink_utils import (
    get_table_from_source,
    insert_into_sink,
)
from feathub.table.schema import Schema


class SourceUtilsTest(unittest.TestCase):
    def test_kafka_source(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)
        schema = Schema(["id", "val", "ts"], [String, Int64, Int64])
        source = KafkaSource(
            "kafka_source",
            bootstrap_server="localhost:9092",
            topic="test-topic",
            key_format="json",
            value_format="json",
            schema=schema,
            consumer_group="test-group",
            keys=["id1", "id2"],
            timestamp_field="ts",
            timestamp_format="epoch",
            consumer_properties={"consumer.key": "value"},
            startup_mode="timestamp",
            startup_datetime=datetime.strptime(
                "2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
            ),
        )

        with patch.object(t_env, "from_descriptor") as from_descriptor:
            get_table_from_source(t_env, source)
            flink_table_descriptor: NativeFlinkTableDescriptor = (
                from_descriptor.call_args[0][0]
            )

            expected_col_strs = [
                "`id` STRING",
                "`val` BIGINT",
                "`ts` BIGINT",
            ]
            schema_str = str(flink_table_descriptor.get_schema())
            for col_str in expected_col_strs:
                self.assertIn(col_str, schema_str)

            expected_options = {
                "connector": "kafka",
                "topic": "test-topic",
                "properties.bootstrap.servers": "localhost:9092",
                "properties.group.id": "test-group",
                "properties.consumer.key": "value",
                "value.format": "json",
                "scan.startup.mode": "timestamp",
                "scan.startup.timestamp-millis": "1640966400000",
                "scan.topic-partition-discovery.interval": "300000 ms",
                "key.format": "json",
                "key.fields": "id1;id2",
                "value.fields-include": "EXCEPT_KEY",
            }
            self.assertEquals(
                expected_options, dict(flink_table_descriptor.get_options())
            )


class SinkUtilTest(unittest.TestCase):
    def test_kafka_sink(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)
        sink = KafkaSink(
            bootstrap_server="localhost:9092",
            topic="test-topic",
            key_format="json",
            value_format="json",
            producer_properties={"producer.key": "value"},
        )

        table = Mock()
        with patch.object(table, "execute_insert") as execute_insert:
            insert_into_sink(t_env, table, sink, ("id",))
            flink_table_descriptor: NativeFlinkTableDescriptor = (
                execute_insert.call_args[0][0]
            )

            expected_options = {
                "connector": "kafka",
                "topic": "test-topic",
                "properties.bootstrap.servers": "localhost:9092",
                "properties.producer.key": "value",
                "value.format": "json",
                "key.format": "json",
                "key.fields": "id",
                "value.fields-include": "EXCEPT_KEY",
            }
            self.assertEquals(
                expected_options, dict(flink_table_descriptor.get_options())
            )


# TODO: Start a Kafka in memory with Kafka test suite for testing.
class SourceSinkITTest(unittest.TestCase):
    kafka_bootstrap_servers = os.environ.get("IT_KAFKA_BOOTSTRAP_SERVERS", None)
    kafka_topic = os.environ.get("IT_KAFKA_TOPIC", None)

    @unittest.skipUnless(
        kafka_bootstrap_servers is not None and kafka_topic is not None,
        "Skip the test unless environment variable IT_KAFKA_BOOTSTRAP_SERVERS and "
        "IT_KAFKA_TOPIC are set.",
    )
    def test_kafka_source_sink(self):
        # Produce data with kafka sink
        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)
        test_time = datetime.now()

        row_data = [
            (1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")),
            (2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")),
            (3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")),
        ]
        table = t_env.from_elements(
            row_data,
            DataTypes.ROW(
                [
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("val", DataTypes.BIGINT()),
                    DataTypes.FIELD("ts", DataTypes.STRING()),
                ]
            ),
        )

        sink = KafkaSink(
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.kafka_topic,
            key_format="json",
            value_format="json",
        )

        insert_into_sink(t_env, table, sink, ("id",)).wait()

        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.kafka_topic,
            key_format="json",
            value_format="json",
            schema=Schema(["id", "val", "ts"], [Int64, Int64, String]),
            consumer_group="test-group",
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            startup_mode="timestamp",
            startup_datetime=test_time,
        )

        expected_rows = {Row(*data) for data in row_data}
        table = get_table_from_source(t_env, source)
        table = table.drop_columns(EVENT_TIME_ATTRIBUTE_NAME)
        table_result = table.execute()
        result_rows = set()
        with table_result.collect() as results:
            for idx, row in enumerate(results):
                result_rows.add(row)
                if idx == len(row_data) - 1:
                    table_result.get_job_client().cancel().result()
                    break

        self.assertEquals(expected_rows, result_rows)

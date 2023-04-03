#  Copyright 2022 The FeatHub Authors
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
from datetime import datetime, timezone
from typing import cast
from unittest.mock import patch

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    TableDescriptor as NativeFlinkTableDescriptor,
    StreamTableEnvironment,
)

from feathub.common.types import Int64, String
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.processors.flink.table_builder.source_sink_utils import (
    get_table_from_source,
    insert_into_sink,
)
from feathub.processors.flink.table_builder.tests.mock_table_descriptor import (
    MockTableDescriptor,
)
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class KafkaSourceSinkTest(unittest.TestCase):
    env = None
    t_env = None

    @classmethod
    def setUpClass(cls) -> None:
        # Due to the resource leak in PyFlink StreamExecutionEnvironment and
        # StreamTableEnvironment https://issues.apache.org/jira/browse/FLINK-30258.
        # We want to share env and t_env across all the tests in one class to mitigate
        # the leak.
        # TODO: After the ticket is resolved, we should clean up the resource in
        #  StreamExecutionEnvironment and StreamTableEnvironment after every test to
        #  fully avoid resource leak.
        cls.env = StreamExecutionEnvironment.get_execution_environment()
        cls.t_env = StreamTableEnvironment.create(cls.env)

    @classmethod
    def tearDownClass(cls) -> None:
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")

    def test_kafka_source(self):
        schema = Schema(["id1", "id2", "val", "ts"], [String, String, Int64, Int64])
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
            startup_datetime=datetime(
                year=2022,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                tzinfo=timezone.utc,
            ),
        )

        with patch.object(
            self.t_env, "from_descriptor"
        ) as from_descriptor, patch.object(self.t_env, "from_path"):
            get_table_from_source(self.t_env, source)
            flink_table_descriptor: NativeFlinkTableDescriptor = (
                from_descriptor.call_args[0][0]
            )

            expected_col_strs = [
                "`id1` STRING",
                "`id2` STRING",
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
                "scan.startup.timestamp-millis": "1640995200000",
                "scan.topic-partition-discovery.interval": "300000 ms",
                "key.format": "json",
                "key.fields": "id1;id2",
                "value.fields-include": "EXCEPT_KEY",
            }
            self.assertEquals(
                expected_options, dict(flink_table_descriptor.get_options())
            )

            bounded_source = source.get_bounded_view()
            get_table_from_source(self.t_env, cast(FeatureTable, bounded_source))
            flink_table_descriptor = from_descriptor.call_args[0][0]

            expected_col_strs = [
                "`id1` STRING",
                "`id2` STRING",
                "`val` BIGINT",
                "`ts` BIGINT",
            ]
            schema_str = str(flink_table_descriptor.get_schema())
            for col_str in expected_col_strs:
                self.assertIn(col_str, schema_str)

            expected_options = {
                "connector": "bounded-kafka",
                "topic": "test-topic",
                "properties.bootstrap.servers": "localhost:9092",
                "properties.group.id": "test-group",
                "properties.consumer.key": "value",
                "value.format": "json",
                "scan.startup.mode": "timestamp",
                "scan.startup.timestamp-millis": "1640995200000",
                "scan.topic-partition-discovery.interval": "300000 ms",
                "key.format": "json",
                "key.fields": "id1;id2",
                "value.fields-include": "EXCEPT_KEY",
            }
            self.assertEquals(
                expected_options, dict(flink_table_descriptor.get_options())
            )

    def test_kafka_sink(self):
        sink = KafkaSink(
            bootstrap_server="localhost:9092",
            topic="test-topic",
            key_format="json",
            value_format="json",
            producer_properties={"producer.key": "value"},
        )

        table = self.t_env.from_elements([(1,)])
        with patch.object(table, "execute_insert") as execute_insert:
            descriptor: TableDescriptor = MockTableDescriptor(keys=["id"])
            insert_into_sink(self.t_env, table, descriptor, sink)
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

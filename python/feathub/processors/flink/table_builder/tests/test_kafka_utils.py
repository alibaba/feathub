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
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    TableDescriptor as NativeFlinkTableDescriptor,
    DataTypes,
)
from testcontainers.kafka import KafkaContainer

from feathub.common.types import Int64, String
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.processors.flink.table_builder.flink_table_builder import FlinkTableBuilder
from feathub.processors.flink.table_builder.flink_table_builder_constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.table_builder.source_sink_utils import (
    get_table_from_source,
    insert_into_sink,
)
from feathub.registries.local_registry import LocalRegistry
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
            t_env, "create_temporary_table"
        ) as create_temporary_table, patch.object(t_env, "from_path"):
            get_table_from_source(t_env, source)
            flink_table_descriptor: NativeFlinkTableDescriptor = (
                create_temporary_table.call_args[0][1]
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
                "scan.startup.timestamp-millis": "1640995200000",
                "scan.topic-partition-discovery.interval": "300000 ms",
                "key.format": "json",
                "key.fields": "id1;id2",
                "value.fields-include": "EXCEPT_KEY",
            }
            self.assertEquals(
                expected_options, dict(flink_table_descriptor.get_options())
            )

            get_table_from_source(t_env, source, True)
            flink_table_descriptor = create_temporary_table.call_args[0][1]

            expected_col_strs = [
                "`id` STRING",
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

        table = t_env.from_elements([(1,)])
        with patch.object(
            t_env, "create_temporary_table"
        ) as create_temporary_table, patch.object(table, "execute_insert"):
            insert_into_sink(t_env, table, sink, ("id",))
            flink_table_descriptor: NativeFlinkTableDescriptor = (
                create_temporary_table.call_args[0][1]
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


class SourceSinkITTest(unittest.TestCase):
    kafka_container: KafkaContainer = None

    def __init__(self, methodName: str):
        super().__init__(methodName)
        self.kafka_bootstrap_servers = None
        self.topic_name = methodName

    @classmethod
    def setUpClass(cls) -> None:
        cls.kafka_container = KafkaContainer()
        cls.kafka_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.kafka_container.stop()

    def setUp(self) -> None:
        self.kafka_bootstrap_servers = (
            SourceSinkITTest.kafka_container.get_bootstrap_server()
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
            topic=self.topic_name,
            key_format="json",
            value_format="json",
        )

        insert_into_sink(t_env, table, sink, ("id",)).wait()

        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.topic_name,
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

    def test_bounded_kafka_source(self):
        # Produce data with kafka sink
        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)
        table_builder = FlinkTableBuilder(t_env, LocalRegistry({}))
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
            topic=self.topic_name,
            key_format="json",
            value_format="json",
        )

        insert_into_sink(t_env, table, sink, ("id",)).wait()

        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.topic_name,
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

        features = DerivedFeatureView(
            "feature_view", source, features=[], keep_source_fields=True
        )
        expected_rows = {Row(*data) for data in row_data}
        table = table_builder.build(features)
        table_result = table.execute()
        with table_result.collect() as results:
            result_rows = set(results)
        self.assertEquals(expected_rows, result_rows)

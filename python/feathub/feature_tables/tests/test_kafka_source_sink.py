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
from abc import ABC
from datetime import datetime

import pandas as pd
from testcontainers.kafka import KafkaContainer

from feathub.common import types
from feathub.common.types import Int64
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class KafkaSourceTest(unittest.TestCase):
    def test_get_bounded_feature_table(self):
        source = KafkaSource(
            "source",
            "bootstrap_server",
            "topic",
            None,
            "csv",
            Schema.new_builder().column("x", Int64).column("y", Int64).build(),
            "consumer_group",
        )
        self.assertFalse(source.is_bounded())

        bounded_source = source.get_bounded_view()
        self.assertTrue(bounded_source.is_bounded())

        source_json = source.to_json()
        source_json.pop("is_bounded")
        bounded_source_json = bounded_source.to_json()
        bounded_source_json.pop("is_bounded")

        self.assertEqual(source_json, bounded_source_json)


class KafkaSourceSinkITTest(ABC, FeathubITTestBase):
    kafka_container = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.kafka_container = KafkaContainer()
        cls.kafka_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.kafka_container.stop()

    def test_kafka_source_sink(self):
        input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=["id", "val", "ts"],
        )

        schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )

        topic_name, start_time = self._produce_data_to_kafka(input_data, schema)

        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_container.get_bootstrap_server(),
            topic=topic_name,
            key_format="json",
            value_format="json",
            schema=schema,
            consumer_group="test-group",
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            startup_mode="timestamp",
            startup_datetime=start_time,
        )

        result_df = (
            self.client.get_features(source)
            .to_pandas(True)
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result_df = (
            input_data.copy().sort_values(by=["id"]).reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_bounded_kafka_source(self):
        input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=["id", "val", "ts"],
        )

        schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )

        topic_name, start_time = self._produce_data_to_kafka(input_data, schema)

        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_container.get_bootstrap_server(),
            topic=topic_name,
            key_format="json",
            value_format="json",
            schema=schema,
            consumer_group="test-group",
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            startup_mode="timestamp",
            startup_datetime=start_time,
            is_bounded=True,
        )

        features = DerivedFeatureView(
            "feature_view", source, features=[], keep_source_fields=True
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result_df = (
            input_data.copy().sort_values(by=["id"]).reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def _produce_data_to_kafka(self, input_data: pd.DataFrame, schema: Schema):
        source = self.create_file_source(
            input_data,
            keys=["id"],
            schema=schema,
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        topic_name = self.generate_random_name("kafka")

        sink = KafkaSink(
            bootstrap_server=self.kafka_container.get_bootstrap_server(),
            topic=topic_name,
            key_format="json",
            value_format="json",
        )

        start_time = datetime.now()

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        return topic_name, start_time

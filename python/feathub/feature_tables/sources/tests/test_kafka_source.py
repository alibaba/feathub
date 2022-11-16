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

from feathub.common.types import Int64
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.table.schema import Schema


class TestKafkaSource(unittest.TestCase):
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

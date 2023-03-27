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
import unittest
from unittest.mock import patch

from feathub.common.exceptions import FeathubException
from feathub.common.types import String
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.processors.flink import flink_table
from feathub.processors.flink.flink_processor import FlinkProcessor
from feathub.processors.flink.flink_table import FlinkTable
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class FlinkTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = LocalRegistry(props={})
        self.processor = FlinkProcessor(
            props={
                "processor.flink.rest.address": "127.0.0.1",
                "processor.flink.rest.port": 1234,
            },
            registry=self.registry,
        )

    def test_to_pandas_force_bounded(self):

        source = DataGenSource(
            "source",
            Schema.new_builder().column("x", String).build(),
        )

        source_2 = DataGenSource(
            "source_2",
            Schema.new_builder().column("x", String).column("y", String).build(),
            keys=["x"],
        )

        features = DerivedFeatureView(
            "sliding", source, ["source_2.y"], keep_source_fields=True
        )
        built_features = self.registry.build_features([source_2, features])[1]
        table = FlinkTable(
            flink_processor=self.processor,
            feature=built_features,
            keys=None,
            start_datetime=None,
            end_datetime=None,
        )

        with self.assertRaises(FeathubException) as cm:
            table.to_pandas()

        self.assertIn(
            "Unbounded table cannot be converted to Pandas DataFrame.",
            cm.exception.args[0],
        )

        with patch.object(
            self.processor.flink_table_builder, "build"
        ) as build_method, patch.object(flink_table, "flink_table_to_pandas") as _:
            table.to_pandas(force_bounded=True)
            self.assertIsInstance(
                build_method.call_args[1]["features"], TableDescriptor
            )
            self.assertTrue(build_method.call_args[1]["features"].is_bounded())

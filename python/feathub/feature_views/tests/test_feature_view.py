# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from feathub.common.exceptions import FeathubException
from feathub.common.types import Int64
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.feature import Feature
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.registries.local_registry import LocalRegistry
from feathub.common import types
from feathub.table.schema import Schema


class FeatureViewTest(unittest.TestCase):
    def setUp(self):
        self.registry = LocalRegistry(config={})

    def test_features(self):
        source = FileSystemSource(
            name="source_1",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema([], []),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform="CAST(fare_amount AS FLOAT) + 1",
        )

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform="CAST(fare_amount AS FLOAT) + 2",
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                feature_1,
                feature_2,
            ],
            keep_source_fields=True,
        )

        feature_3 = Feature(
            name="feature_3",
            dtype=types.Bool,
            transform="CAST(trip_distance AS FLOAT)>30",
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=[
                "feature_1",
                feature_3,
            ],
            keep_source_fields=True,
        )
        built_feature_view = self.registry.build_features([feature_view_2])[0]

        self.assertEqual(feature_1, built_feature_view.get_feature("feature_1"))
        self.assertEqual(feature_2, built_feature_view.get_feature("feature_2"))
        self.assertEqual(feature_3, built_feature_view.get_feature("feature_3"))

    def test_unresolved_feature_view(self):
        feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform="CAST(fare_amount AS FLOAT) + 1",
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source="source_1",
            features=[
                feature_1,
            ],
            keep_source_fields=True,
        )

        self.assertTrue(feature_view_1.is_unresolved())
        self.assertIsNone(feature_view_1.timestamp_field)

    def test_feature_view_boundedness(self):
        source = DataGenSource(
            name="source_1",
            schema=Schema(["id", "val1"], [Int64, Int64]),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        source_2 = DataGenSource(
            name="source_2",
            schema=Schema(["id", "val2"], [Int64, Int64]),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=["source_2.val2"],
            keep_source_fields=True,
        )

        built_feature_view_1 = self.registry.build_features([source_2, feature_view_1])[
            1
        ]
        self.assertFalse(built_feature_view_1.is_bounded())

        with self.assertRaises(RuntimeError) as cm:
            feature_view_1.get_bounded_view()
        self.assertIn("This feature view is unresolved.", cm.exception.args[0])

        bounded_feature_view_1 = built_feature_view_1.get_bounded_view()
        self.assertTrue(bounded_feature_view_1.is_bounded())

        bounded_source = FileSystemSource(
            name="bounded_source",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema(["id", "val1"], [Int64, Int64]),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )
        bounded_source_2 = FileSystemSource(
            name="bounded_source_2",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema(["id", "val2"], [Int64, Int64]),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=bounded_source,
            features=["bounded_source_2.val2"],
            keep_source_fields=True,
        )
        built_feature_view_2 = self.registry.build_features(
            [bounded_source_2, feature_view_2]
        )[1]
        self.assertTrue(built_feature_view_2.is_bounded())

    def test_bounded_left_table_join_unbounded_right_table(self):
        source = DataGenSource(
            name="source_1",
            schema=Schema(["id", "val1"], [Int64, Int64]),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
            number_of_rows=1,
        )

        source_2 = DataGenSource(
            name="source_2",
            schema=Schema(["id", "val2"], [Int64, Int64]),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=["source_2.val2"],
            keep_source_fields=True,
        )

        with self.assertRaises(FeathubException) as cm:
            _ = self.registry.build_features([source_2, feature_view_1])[1]

        self.assertIn(
            "Joining a bounded left table with an unbounded right table is currently "
            "not supported.",
            cm.exception.args[0],
        )

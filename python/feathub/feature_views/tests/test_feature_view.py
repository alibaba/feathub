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
            transform="cast_float(fare_amount) + 1",
        )

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform="cast_float(fare_amount) + 2",
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
            transform="cast_float(trip_distance)>30",
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

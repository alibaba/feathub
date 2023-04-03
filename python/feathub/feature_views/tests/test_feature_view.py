# Copyright 2022 The FeatHub Authors
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
from feathub.common.types import Int64, String
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.feature import Feature
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.registries.local_registry import LocalRegistry
from feathub.common import types
from feathub.table.schema import Schema


class FeatureViewTest(unittest.TestCase):
    def setUp(self):
        self.registry = LocalRegistry(props={})

    def test_features(self):
        source = FileSystemSource(
            name="source_1",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema(
                ["lpep_dropoff_datetime", "fare_amount", "trip_distance"],
                [Int64, Int64, Int64],
            ),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        feature_1 = Feature(
            name="feature_1",
            transform="CAST(fare_amount AS FLOAT) + 1",
        )

        feature_2 = Feature(
            name="feature_2",
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

        expected_feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform="`feature_1`",
        )
        self.assertEqual(
            expected_feature_1, built_feature_view.get_feature("feature_1")
        )
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
            schema=Schema(
                ["id", "val1", "lpep_dropoff_datetime"], [Int64, Int64, Int64]
            ),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        source_2 = DataGenSource(
            name="source_2",
            schema=Schema(
                ["id", "val2", "lpep_dropoff_datetime"], [Int64, Int64, Int64]
            ),
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
            schema=Schema(
                ["id", "val1", "lpep_dropoff_datetime"], [Int64, Int64, Int64]
            ),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )
        bounded_source_2 = FileSystemSource(
            name="bounded_source_2",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema(
                ["id", "val2", "lpep_dropoff_datetime"], [Int64, Int64, Int64]
            ),
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

    def test_duplicate_feature_names_throw_exception(self):
        source = DataGenSource(
            name="source",
            schema=Schema(
                ["id", "val1", "lpep_dropoff_datetime"], [Int64, Int64, Int64]
            ),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        with self.assertRaises(FeathubException) as ctx:
            DerivedFeatureView(
                name="feature_view",
                source=source,
                features=[
                    Feature(name="a", dtype=types.Int32, transform="val1 + 1"),
                    Feature(name="a", dtype=types.Int32, transform="val1 + 2"),
                ],
                keep_source_fields=True,
            )
        self.assertIn("contains duplicated feature name", ctx.exception.args[0])

    def test_get_output_fields(self):
        field_names = ["id", "val1", "val2", "val3", "time"]
        source = DataGenSource(
            name="source_1",
            schema=Schema(field_names, [Int64, Int64, Int64, Int64, String]),
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        feature_1 = Feature(
            name="derived_val_1", dtype=types.Int64, transform="val1 + 1", keys=["val1"]
        )

        feature_2 = Feature(
            name="derived_val_2", dtype=types.Int64, transform="val2 + 1", keys=["val2"]
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=["time", feature_1, feature_2],
            keep_source_fields=True,
        )

        built_feature_view_1 = feature_view_1.build(self.registry)

        self.assertEqual(
            [
                "id",
                "val1",
                "val2",
                "val3",
                "time",
                "derived_val_1",
                "derived_val_2",
            ],
            built_feature_view_1.get_output_fields(field_names),  # type: ignore
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=source,
            features=["time", feature_1, feature_2],
            keep_source_fields=False,
        )

        built_feature_view_2 = feature_view_2.build(self.registry)

        self.assertEqual(
            ["id", "val1", "val2", "time", "derived_val_1", "derived_val_2"],
            built_feature_view_2.get_output_fields(field_names),  # type: ignore
        )

        feature_view_3 = DerivedFeatureView(
            name="feature_view_3",
            source=source,
            features=["time", "val3", "val2", "val1", "id"],
            keep_source_fields=False,
        )

        built_feature_view_3 = feature_view_3.build(self.registry)

        self.assertEqual(
            ["time", "val3", "val2", "val1", "id"],
            built_feature_view_3.get_output_fields(field_names),  # type: ignore
        )

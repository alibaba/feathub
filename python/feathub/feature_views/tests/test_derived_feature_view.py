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
from abc import ABC
from typing import Optional

import pandas as pd

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.types import String, Float64
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class DerivedFeatureViewTest(unittest.TestCase):
    def setUp(self):
        self.registry = LocalRegistry(props={})
        self.source = FileSystemSource(
            name="source_1",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema(
                ["id", "fare_amount", "lpep_dropoff_datetime"],
                [types.Int32, types.Int32, types.String],
            ),
            keys=["id"],
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        self.source_2 = FileSystemSource(
            name="source_2",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema(
                ["id", "distance", "lpep_dropoff_datetime"],
                [types.Int32, types.Int32, types.String],
            ),
            keys=["id"],
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

    def test_derived_feature_view_order(self):
        join_feature = "source_2.distance"
        expr_feature_1 = Feature(name="id_plus_distance", transform="id + distance")
        over_feature_1 = Feature(
            name="over_window",
            transform=OverWindowTransform(
                expr="fare_amount + distance", agg_func="AVG"
            ),
            keys=["id"],
        )
        over_feature_2 = Feature(
            name="over_window_2",
            transform=OverWindowTransform(expr="fare_amount", agg_func="AVG"),
            keys=["id_plus_distance"],
        )
        expr_feature_2 = Feature(name="expression_feature", transform="over_window + 1")
        feature_view = DerivedFeatureView(
            name="derived_feature_view",
            source=self.source,
            features=[
                join_feature,
                expr_feature_1,
                over_feature_1,
                over_feature_2,
                expr_feature_2,
            ],
        )

        self.registry.build_features([self.source_2])
        built_feature_view = feature_view.build(self.registry)
        self.assertTrue(
            {
                "distance",
                "id_plus_distance",
                "over_window",
                "over_window_2",
                "expression_feature",
            }.issubset(set([f.name for f in built_feature_view.get_output_features()]))
        )

        # expr_feature_1 depends on the join_feature. Exception should be raised if we
        # put expr_feature_1 before join_feature.
        with self.assertRaises(FeathubException):
            DerivedFeatureView(
                name="derived_feature_view",
                source=self.source,
                features=[
                    expr_feature_1,
                    join_feature,
                    over_feature_1,
                    over_feature_2,
                    expr_feature_2,
                ],
            ).build(self.registry)

        # expr_feature_2 depends on the over_feature_1. Exception should be raised if we
        # put expr_feature_2 before over_feature_1.
        with self.assertRaises(FeathubException):
            DerivedFeatureView(
                name="derived_feature_view",
                source=self.source,
                features=[
                    join_feature,
                    expr_feature_1,
                    expr_feature_2,
                    over_feature_1,
                    over_feature_2,
                ],
            ).build(self.registry)


class DerivedFeatureViewITTest(ABC, FeathubITTestBase):
    def test_derived_feature_view_filter_expr(self):
        source = self.create_file_source(self.input_data.copy())

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[],
            keep_source_fields=True,
            filter_expr="name = 'Alex' AND cost > 200",
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = pd.DataFrame(
            [
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_filter_is_null(self):
        source = self.create_file_source(self.input_data.copy())

        def alex_to_null(row: pd.Series) -> Optional[str]:
            if row["name"] == "Alex":
                return None
            return row["name"]

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="name_without_alex",
                    dtype=String,
                    transform=PythonUdfTransform(alex_to_null),
                    keys=["name"],
                )
            ],
            keep_source_fields=True,
            filter_expr="name_without_alex IS NOT NULL",
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = pd.DataFrame(
            [
                ["Emma", 400, 250, "2022-01-01 08:02:00", "Emma"],
                ["Emma", 200, 250, "2022-01-02 08:04:00", "Emma"],
                ["Jack", 500, 500, "2022-01-03 08:05:00", "Jack"],
            ],
            columns=["name", "cost", "distance", "time", "name_without_alex"],
        )

        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_filter_float_is_null(self):
        source = self.create_file_source(self.input_data.copy())

        def alex_cost_to_null(row: pd.Series) -> Optional[float]:
            if row["name"] == "Alex":
                return None
            return float(row["cost"])

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="float_cost",
                    dtype=Float64,
                    transform=PythonUdfTransform(alex_cost_to_null),
                    keys=["name"],
                )
            ],
            keep_source_fields=True,
            filter_expr="float_cost IS NOT NULL",
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = pd.DataFrame(
            [
                ["Emma", 400, 250, "2022-01-01 08:02:00", 400.0],
                ["Emma", 200, 250, "2022-01-02 08:04:00", 200.0],
                ["Jack", 500, 500, "2022-01-03 08:05:00", 500.0],
            ],
            columns=["name", "cost", "distance", "time", "float_cost"],
        )

        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_reserved_keyword_as_field_name(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "timestamp"],
        )

        schema = (
            Schema.new_builder()
            .column("name", types.String)
            .column("cost", types.Int64)
            .column("distance", types.Int64)
            .column("timestamp", types.String)
            .build()
        )
        source = self.create_file_source(df, schema=schema, timestamp_field="timestamp")

        features = DerivedFeatureView(
            name="feature_view_2",
            source=source,
            features=["timestamp"],
            keep_source_fields=False,
        )

        self.client.build_features([features])

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["timestamp"])
            .reset_index(drop=True)
        )

        expected_result_df = result_df[["timestamp"]]

        self.assertTrue(expected_result_df.equals(result_df))

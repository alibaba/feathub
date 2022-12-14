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
from math import sqrt

import pandas as pd
from datetime import timedelta

from feathub.common.test_utils import LocalProcessorTestCase
from feathub.common import types
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform


class DerivedFeatureViewTest(LocalProcessorTestCase):
    def setUp(self):
        super().setUp()
        self.input_data = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

    def tearDown(self):
        super().tearDown()

    def test_keep_source(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            dtype=types.Float32,
            transform="cost / distance + 10",
        )
        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                f_cost_per_mile,
            ],
            keep_source_fields=True,
        )

        result_df = self.processor.get_table(feature_view).to_pandas()

        expected_result_df = df
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_derived_feature_view(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            dtype=types.Float32,
            transform="cost / distance + 10",
        )

        f_total_cost = Feature(
            name="total_cost",
            dtype=types.Float32,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_avg_cost = Feature(
            name="avg_cost",
            dtype=types.Float32,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="AVG",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_max_cost = Feature(
            name="max_cost",
            dtype=types.Float32,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="MAX",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_min_cost = Feature(
            name="min_cost",
            dtype=types.Float32,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="MIN",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                f_cost_per_mile,
                f_total_cost,
                f_avg_cost,
                f_max_cost,
                f_min_cost,
            ],
            keep_source_fields=False,
        )

        result_df = self.processor.get_table(features=features).to_pandas()
        expected_result_df = df
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        )
        expected_result_df["total_cost"] = pd.Series([100, 400, 400, 600, 500, 900])
        expected_result_df["avg_cost"] = pd.Series(
            [100.0, 400.0, 200.0, 300.0, 500.0, 450.0]
        )
        expected_result_df["max_cost"] = pd.Series([100, 400, 300, 400, 500, 600])
        expected_result_df["min_cost"] = pd.Series([100, 400, 100, 200, 500, 300])
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)

        self.assertIsNone(source.keys)
        self.assertListEqual(["name"], features.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_window_agg_transform_with_millisecond_window_size(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:00:00.001"],
                ["Emma", 400, 250, "2022-01-01 08:00:00.002"],
                ["Alex", 300, 200, "2022-01-01 08:00:00.003"],
                ["Emma", 200, 250, "2022-01-01 08:00:00.004"],
                ["Jack", 500, 500, "2022-01-01 08:00:00.005"],
                ["Alex", 600, 800, "2022-01-01 08:00:00.006"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        source = self._create_file_source(df, timestamp_format="%Y-%m-%d %H:%M:%S.%f")

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_sum",
                    dtype=types.Int64,
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(milliseconds=3),
                    ),
                ),
            ],
        )

        expected_result_df = df
        expected_result_df["cost_sum"] = pd.Series([100, 400, 400, 600, 500, 900])
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)

        result_df = self.processor.get_table(features=features).to_pandas()
        self.assertTrue(expected_result_df.equals(result_df))

    def test_join_transform(self):
        df_1 = self.input_data.copy()
        source = self._create_file_source(df_1)
        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="cost",
                    dtype=types.Float32,
                    transform="cost",
                ),
                Feature(
                    name="distance",
                    dtype=types.Float32,
                    transform="distance",
                ),
            ],
            keep_source_fields=True,
        )

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Emma", 400.0, "2022-01-01 09:02:00"],
                ["Alex", 200.0, "2022-01-02 09:03:00"],
                ["Emma", 300.0, "2022-01-02 09:04:00"],
                ["Jack", 500.0, "2022-01-03 09:05:00"],
                ["Alex", 450.0, "2022-01-03 09:06:00"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self._create_file_source(df_2)
        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=source_2,
            features=[
                Feature(
                    name="name",
                    dtype=types.Float32,
                    transform="name",
                ),
                Feature(
                    name="avg_cost",
                    dtype=types.Float32,
                    transform="avg_cost",
                    keys=["name"],
                ),
            ],
            keep_source_fields=False,
        )

        feature_view_3 = DerivedFeatureView(
            name="feature_view_3",
            source=feature_view_1,
            features=[
                Feature(
                    name="cost",
                    dtype=types.Float32,
                    transform="cost",
                ),
                "distance",
                "feature_view_2.avg_cost",
                Feature(
                    name="derived_cost",
                    dtype=types.Float32,
                    transform="avg_cost * distance",
                ),
            ],
            keep_source_fields=False,
        )

        [built_feature_view_2, built_feature_view_3] = self.registry.build_features(
            [feature_view_2, feature_view_3]
        )

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 20000.0, 100000.0, None, 160000.0]
        )
        result_df = self.processor.get_table(features=built_feature_view_3).to_pandas()

        self.assertIsNone(feature_view_1.keys)
        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertListEqual(["name"], built_feature_view_3.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_python_udf_transform_on_over_window_transform(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:00:00.001"],
                ["Emma", 400, 250, "2022-01-01 08:00:00.002"],
                ["Alex", 300, 200, "2022-01-01 08:00:00.003"],
                ["Emma", 200, 250, "2022-01-01 08:00:00.004"],
                ["Jack", 500, 500, "2022-01-01 08:00:00.005"],
                ["Alex", 600, 800, "2022-01-01 08:00:00.006"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        source = self._create_file_source(df, timestamp_format="%Y-%m-%d %H:%M:%S.%f")

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="lower_name",
                    dtype=types.String,
                    transform=PythonUdfTransform(lambda row: row["name"].lower()),
                ),
                Feature(
                    name="cost_sum",
                    dtype=types.Int64,
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["lower_name"],
                        window_size=timedelta(milliseconds=3),
                    ),
                ),
                Feature(
                    name="cost_sum_sqrt",
                    dtype=types.Float64,
                    transform=PythonUdfTransform(lambda row: sqrt(row["cost_sum"])),
                ),
            ],
        )

        expected_result_df = df
        expected_result_df["lower_name"] = expected_result_df["name"].apply(
            lambda x: x.lower()
        )
        expected_result_df["cost_sum"] = pd.Series([100, 400, 400, 600, 500, 900])
        expected_result_df["cost_sum_sqrt"] = expected_result_df["cost_sum"].apply(
            lambda x: sqrt(x)
        )
        expected_result_df.drop(["name", "cost", "distance"], axis=1, inplace=True)

        result_df = self.processor.get_table(features=features).to_pandas()
        self.assertTrue(expected_result_df.equals(result_df))

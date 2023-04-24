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
from abc import ABC
from datetime import timedelta
from math import sqrt

import pandas as pd

from feathub.common.types import Int64, String, Float64
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class OverWindowTransformITTest(ABC, FeathubITTestBase):
    def test_over_window_transform(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            transform="CAST(cost AS DOUBLE) / CAST(distance AS DOUBLE) + 10",
        )

        f_total_cost = Feature(
            name="total_cost",
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_avg_cost = Feature(
            name="avg_cost",
            transform=OverWindowTransform(
                expr="cost",
                agg_func="AVG",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_max_cost = Feature(
            name="max_cost",
            transform=OverWindowTransform(
                expr="cost",
                agg_func="MAX",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_min_cost = Feature(
            name="min_cost",
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

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

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
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        self.assertIsNone(source.keys)
        self.assertListEqual(["name"], features.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_with_unsupported_agg_func(self):
        with self.assertRaises(ValueError):
            Feature(
                name="feature_1",
                transform=OverWindowTransform(
                    "cost", "unsupported_agg", window_size=timedelta(days=2)
                ),
            )

    def test_over_window_transform_without_key(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
            ),
        )

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
            keep_source_fields=True,
        )

        expected_result_df = df
        expected_result_df["total_cost"] = pd.Series([100, 500, 800, 1000, 1000, 1600])
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_without_window_size_and_limit(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=OverWindowTransform(
                expr="cost", agg_func="SUM", group_by_keys=["name"]
            ),
        )

        expected_result_df = df
        expected_result_df["total_cost"] = pd.Series([100, 400, 400, 600, 500, 1000])
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_with_limit(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=OverWindowTransform(
                expr="cost", agg_func="SUM", group_by_keys=["name"], limit=2
            ),
        )

        expected_result_df = df
        expected_result_df["total_cost"] = pd.Series([100, 400, 400, 600, 500, 900])
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_with_millis_window_size(self):
        df, schema = self.create_input_data_and_schema_with_millis_time_span()

        source = self.create_file_source(
            df, timestamp_format="%Y-%m-%d %H:%M:%S.%f", schema=schema
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_sum",
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
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_with_epoch_millis_window_size(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, 1640995200001],
                ["Emma", 400, 250, 1640995200002],
                ["Alex", 300, 200, 1640995200003],
                ["Emma", 200, 250, 1640995200004],
                ["Jack", 500, 500, 1640995200005],
                ["Alex", 600, 800, 1640995200006],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        source = self.create_file_source(
            df,
            timestamp_format="epoch_millis",
            schema=Schema.new_builder()
            .column("name", String)
            .column("cost", Int64)
            .column("distance", Int64)
            .column("time", Int64)
            .build(),
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_sum",
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
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_expression_transform_on_over_window_transform(self):
        df, schema = self.create_input_data_and_schema_with_millis_time_span()

        source = self.create_file_source(
            df, timestamp_format="%Y-%m-%d %H:%M:%S.%f", schema=schema
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_sum",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(milliseconds=3),
                    ),
                ),
                Feature(name="double_cost_sum", transform="cost_sum * 2"),
            ],
        )

        expected_result_df = df
        expected_result_df["cost_sum"] = pd.Series([100, 400, 400, 600, 500, 900])
        expected_result_df["double_cost_sum"] = pd.Series(
            [200, 800, 800, 1200, 1000, 1800]
        )
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_python_udf_transform_on_over_window_transform(self):
        df, schema = self.create_input_data_and_schema_with_millis_time_span()

        source = self.create_file_source(
            df, schema=schema, timestamp_format="%Y-%m-%d %H:%M:%S.%f"
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="lower_name",
                    dtype=String,
                    transform=PythonUdfTransform(lambda row: row["name"].lower()),
                ),
                Feature(
                    name="cost_sum",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["lower_name"],
                        window_size=timedelta(milliseconds=3),
                    ),
                ),
                Feature(
                    name="cost_sum_sqrt",
                    dtype=Float64,
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
        expected_result_df = expected_result_df.sort_values(
            by=["lower_name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["lower_name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_expression_transform_and_over_window_transform(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    "avg_cost",
                    transform=OverWindowTransform(
                        expr="CAST(cost AS DOUBLE)",
                        agg_func="AVG",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                    ),
                ),
                Feature("ten_times_cost", transform="10 * cost"),
            ],
            keep_source_fields=True,
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=["avg_cost", "ten_times_cost"],
        )

        self.client.build_features([feature_view_1, feature_view_2])

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [100.0, 400.0, 200.0, 300.0, 500.0, 450.0]
        )
        expected_result_df["ten_times_cost"] = pd.Series(
            [1000, 4000, 3000, 2000, 5000, 6000]
        )
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features("feature_view_2")
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_with_window_size_and_limit(self):
        df = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Alex", 300.0, "2022-01-01 09:01:30"],
                ["Alex", 200.0, "2022-01-01 09:01:20"],
                ["Emma", 500.0, "2022-01-01 09:02:30"],
                ["Emma", 400.0, "2022-01-01 09:02:00"],
                ["Alex", 200.0, "2022-01-01 09:03:00"],
                ["Emma", 300.0, "2022-01-01 09:04:00"],
                ["Jack", 500.0, "2022-01-01 09:05:00"],
                ["Alex", 450.0, "2022-01-01 09:06:00"],
            ],
            columns=["name", "cost", "time"],
        )

        schema = Schema(["name", "cost", "time"], [String, Float64, String])
        source = self.create_file_source(df, schema=schema)

        expected_df = df.copy()
        expected_df["last_2_last_2_minute_total_cost"] = pd.Series(
            [100.0, 500.0, 300.0, 900.0, 400.0, 500.0, 800.0, 500.0, 450.0]
        )
        expected_df["last_2_last_2_minute_avg_cost"] = pd.Series(
            [100.0, 250.0, 150.0, 450.0, 400.0, 250.0, 400.0, 500.0, 450.0]
        )
        expected_df["last_2_last_2_minute_max_cost"] = pd.Series(
            [100.0, 300.0, 200.0, 500.0, 400.0, 300.0, 500.0, 500.0, 450.0]
        )
        expected_df["last_2_last_2_minute_min_cost"] = pd.Series(
            [100.0, 200.0, 100.0, 400.0, 400.0, 200.0, 300.0, 500.0, 450.0]
        )
        expected_df.drop(["cost"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="last_2_last_2_minute_total_cost",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        limit=2,
                    ),
                ),
                Feature(
                    name="last_2_last_2_minute_avg_cost",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="AVG",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        limit=2,
                    ),
                ),
                Feature(
                    name="last_2_last_2_minute_max_cost",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="MAX",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        limit=2,
                    ),
                ),
                Feature(
                    name="last_2_last_2_minute_min_cost",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="MIN",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        limit=2,
                    ),
                ),
            ],
        )

        table = self.client.get_features(features=features)
        result_df = (
            table.to_pandas().sort_values(by=["name", "time"]).reset_index(drop=True)
        )

        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_first_last_value_with_window_size(self):
        result_df = self._over_window_transform_first_last_value(
            window_size=timedelta(days=3)
        )
        expected_df = self.input_data.copy()
        expected_df["first_time"] = pd.Series(
            [
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-03 08:05:00",
                "2022-01-01 08:01:00",
            ]
        )
        expected_df["last_time"] = pd.Series(
            [
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-02 08:03:00",
                "2022-01-02 08:04:00",
                "2022-01-03 08:05:00",
                "2022-01-03 08:06:00",
            ]
        )
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_first_last_value_with_limit(self):
        result_df = self._over_window_transform_first_last_value(limit=2)
        expected_df = self.input_data.copy()
        expected_df["first_time"] = pd.Series(
            [
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-03 08:05:00",
                "2022-01-02 08:03:00",
            ]
        )
        expected_df["last_time"] = pd.Series(
            [
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-02 08:03:00",
                "2022-01-02 08:04:00",
                "2022-01-03 08:05:00",
                "2022-01-03 08:06:00",
            ]
        )
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_first_last_value_with_window_size_and_limit(self):
        result_df = self._over_window_transform_first_last_value(
            window_size=timedelta(days=2), limit=2
        )
        expected_df = self.input_data.copy()
        expected_df["first_time"] = pd.Series(
            [
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-03 08:05:00",
                "2022-01-02 08:03:00",
            ]
        )
        expected_df["last_time"] = pd.Series(
            [
                "2022-01-01 08:01:00",
                "2022-01-01 08:02:00",
                "2022-01-02 08:03:00",
                "2022-01-02 08:04:00",
                "2022-01-03 08:05:00",
                "2022-01-03 08:06:00",
            ]
        )
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_row_num(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="row_num",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="ROW_NUMBER",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        limit=2,
                    ),
                ),
            ],
        )

        expected_df = df.copy()
        expected_df["row_num"] = pd.Series([1, 1, 2, 2, 1, 2])
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        result_df = (
            self.client.get_features(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_value_counts(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Alex", 100, 100, "2022-01-01 08:01:01"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 100, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        source = self.create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_value_counts_limit",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="VALUE_COUNTS",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        limit=2,
                    ),
                ),
                Feature(
                    name="cost_value_counts",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="VALUE_COUNTS",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                    ),
                ),
            ],
        )

        expected_df = df.copy()
        expected_df["cost_value_counts_limit"] = pd.Series(
            [
                {100: 1},
                {100: 2},
                {400: 1},
                {100: 2},
                {200: 1, 400: 1},
                {500: 1},
                {100: 1, 600: 1},
            ]
        )
        expected_df["cost_value_counts"] = pd.Series(
            [
                {100: 1},
                {100: 2},
                {400: 1},
                {100: 3},
                {200: 1, 400: 1},
                {500: 1},
                {100: 1, 600: 1},
            ]
        )
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        result_df = (
            self.client.get_features(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_filter_expr_with_window_size(self):
        df, result_df = self._over_window_transform_filter_expr(
            window_size=timedelta(minutes=2)
        )
        expected_df = df.copy()
        expected_df["last_2_pay_last_2_minute_total_cost"] = pd.Series(
            [100.0, None, 300.0, None, 400.0, None, 700.0, None, 450.0]
        )
        expected_df.drop(["cost", "action"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_filter_expr_with_limit(self):
        df, result_df = self._over_window_transform_filter_expr(limit=2)
        expected_df = df.copy()
        expected_df["last_2_pay_last_2_minute_total_cost"] = pd.Series(
            [100.0, None, 300.0, None, 400.0, None, 700.0, None, 650.0]
        )
        expected_df.drop(["cost", "action"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_filter_expr_with_window_size_and_limit(self):
        df, result_df = self._over_window_transform_filter_expr(
            window_size=timedelta(minutes=2), limit=2
        )
        expected_df = df.copy()
        expected_df["last_2_pay_last_2_minute_total_cost"] = pd.Series(
            [100.0, None, 300.0, None, 400.0, None, 700.0, None, 450.0]
        )
        expected_df.drop(["cost", "action"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_with_different_criteria(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_all_total_cost = Feature(
            name="all_total_cost",
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
            ),
        )
        f_not_ranged_total_cost = Feature(
            name="not_ranged_total_cost",
            transform=OverWindowTransform(
                expr="cost", agg_func="SUM", group_by_keys=["name"]
            ),
        )
        f_time_window_total_cost = Feature(
            name="time_window_total_cost",
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_row_limit_total_cost = Feature(
            name="row_limit_total_cost",
            transform=OverWindowTransform(
                expr="cost", agg_func="SUM", group_by_keys=["name"], limit=2
            ),
        )
        f_time_window_row_limit_total_cost = Feature(
            name="time_window_row_limit_total_cost",
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                limit=2,
                window_size=timedelta(days=2),
            ),
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                f_all_total_cost,
                f_not_ranged_total_cost,
                f_time_window_total_cost,
                f_row_limit_total_cost,
                f_time_window_row_limit_total_cost,
            ],
        )

        expected_result_df = df
        expected_result_df["all_total_cost"] = pd.Series(
            [100, 500, 800, 1000, 1000, 1600]
        )
        expected_result_df["not_ranged_total_cost"] = pd.Series(
            [100, 400, 400, 600, 500, 1000]
        )
        expected_result_df["time_window_total_cost"] = pd.Series(
            [100, 400, 400, 600, 500, 900]
        )
        expected_result_df["row_limit_total_cost"] = pd.Series(
            [100, 400, 400, 600, 500, 900]
        )
        expected_result_df["time_window_row_limit_total_cost"] = pd.Series(
            [100, 400, 400, 600, 500, 900]
        )
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_on_join_field(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01,09:01:00"],
                ["Emma", 400.0, "2022-01-01,09:02:00"],
                ["Alex", 200.0, "2022-01-02,09:03:00"],
                ["Emma", 300.0, "2022-01-02,09:04:00"],
                ["Jack", 500.0, "2022-01-03,09:05:00"],
                ["Alex", 450.0, "2022-01-03,09:06:00"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self.create_file_source(
            df_2,
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
            timestamp_format="%Y-%m-%d,%H:%M:%S",
            keys=["name"],
        )
        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=source,
            features=[
                Feature(
                    name="cost",
                    transform="cost",
                ),
                "distance",
                f"{source_2.name}.avg_cost",
                Feature(
                    name="derived_cost",
                    transform="avg_cost * distance",
                ),
                Feature(
                    name="last_avg_cost",
                    transform=OverWindowTransform(
                        expr="avg_cost",
                        agg_func="LAST_VALUE",
                        window_size=timedelta(days=2),
                        group_by_keys=["name"],
                        limit=2,
                    ),
                ),
                Feature(
                    name="double_last_avg_cost",
                    transform="last_avg_cost * 2",
                ),
            ],
            keep_source_fields=False,
        )

        [_, built_feature_view_2] = self.client.build_features(
            [source_2, feature_view_2]
        )

        expected_result_df = df_1[["name", "time", "cost", "distance"]]
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 20000.0, 100000.0, None, 160000.0]
        )
        expected_result_df["last_avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["double_last_avg_cost"] = pd.Series(
            [None, None, 200.0, 800.0, None, 400.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=built_feature_view_2)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_feature_with_dtype(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_sum",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                    ),
                    dtype=Float64,
                ),
            ],
        )

        expected_df = df.copy()
        expected_df["cost_sum"] = pd.Series([100, 400, 400, 600, 500, 900]).astype(
            "float64"
        )
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        result_df = (
            self.client.get_features(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(
            expected_df.equals(result_df),
            f"Expected: {expected_df}\n Actual: {result_df}",
        )

    def _over_window_transform_first_last_value(
        self,
        window_size=None,
        limit=None,
    ):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="first_time",
                    transform=OverWindowTransform(
                        expr="`time`",
                        agg_func="FIRST_VALUE",
                        group_by_keys=["name"],
                        window_size=window_size,
                        limit=limit,
                    ),
                ),
                Feature(
                    name="last_time",
                    transform=OverWindowTransform(
                        expr="`time`",
                        agg_func="LAST_VALUE",
                        group_by_keys=["name"],
                        window_size=window_size,
                        limit=limit,
                    ),
                ),
            ],
        )
        result_df = (
            self.client.get_features(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        return result_df

    def _over_window_transform_filter_expr(
        self,
        window_size=None,
        limit=None,
    ):
        df = pd.DataFrame(
            [
                ["Alex", "pay", 100.0, "2022-01-01 09:01:00"],
                ["Alex", "receive", 300.0, "2022-01-01 09:01:30"],
                ["Alex", "pay", 200.0, "2022-01-01 09:01:20"],
                ["Emma", "receive", 500.0, "2022-01-01 09:02:30"],
                ["Emma", "pay", 400.0, "2022-01-01 09:02:00"],
                ["Alex", "receive", 200.0, "2022-01-01 09:03:00"],
                ["Emma", "pay", 300.0, "2022-01-01 09:04:00"],
                ["Jack", "receive", 500.0, "2022-01-01 09:05:00"],
                ["Alex", "pay", 450.0, "2022-01-01 09:06:00"],
            ],
            columns=["name", "action", "cost", "time"],
        )

        schema = Schema(
            ["name", "action", "cost", "time"], [String, String, Float64, String]
        )
        source = self.create_file_source(df, schema=schema, keys=["name"])
        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="last_2_pay_last_2_minute_total_cost",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=window_size,
                        filter_expr="action='pay'",
                        limit=limit,
                    ),
                ),
            ],
        )

        table = self.client.get_features(features=features)
        result_df = (
            table.to_pandas().sort_values(by=["name", "time"]).reset_index(drop=True)
        )
        return df, result_df

    def test_over_window_transform_count(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Alex", 100, 100, "2022-01-01 08:01:01"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 100, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        source = self.create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_count",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="count",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                    ),
                ),
            ],
        )

        expected_df = df.copy()
        expected_df["cost_count"] = pd.Series([1, 2, 1, 3, 2, 1, 2])
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        result_df = (
            self.client.get_features(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_count_with_limit(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Alex", 100, 100, "2022-01-01 08:01:01"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 100, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        source = self.create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_count",
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="count",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        limit=2,
                    ),
                ),
            ],
        )

        expected_df = df.copy()
        expected_df["cost_count"] = pd.Series([1, 2, 1, 2, 2, 1, 2])
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        result_df = (
            self.client.get_features(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_df.equals(result_df))

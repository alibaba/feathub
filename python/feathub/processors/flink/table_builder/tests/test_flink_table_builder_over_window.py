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
from datetime import timedelta
from math import sqrt

import pandas as pd

from feathub.common.types import Int64, String, Float64, MapType
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.processors.flink.flink_table import flink_table_to_pandas
from feathub.processors.flink.table_builder.tests.table_builder_test_utils import (
    FlinkTableBuilderTestBase,
)
from feathub.table.schema import Schema


class FlinkTableBuilderOverWindowTransformTest(FlinkTableBuilderTestBase):
    def test_over_window_transform_with_unsupported_agg_func(self):
        with self.assertRaises(ValueError):
            Feature(
                name="feature_1",
                dtype=Int64,
                transform=OverWindowTransform(
                    "cost", "unsupported_agg", window_size=timedelta(days=2)
                ),
            )

    def test_over_window_transform_without_key(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
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
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_without_window_size_and_limit(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
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
            self.flink_table_builder.build(features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_with_limit(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
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
            self.flink_table_builder.build(features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_over_window_transform_with_window_size(self):
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
                    dtype=Int64,
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
            self.flink_table_builder.build(features=features)
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

        source = self._create_file_source(
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
                    dtype=Int64,
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
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_expression_transform_on_over_window_transform(self):
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
                    dtype=Int64,
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(milliseconds=3),
                    ),
                ),
                Feature(name="double_cost_sum", dtype=Int64, transform="cost_sum * 2"),
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
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
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
                    dtype=String,
                    transform=PythonUdfTransform(lambda row: row["name"].lower()),
                ),
                Feature(
                    name="cost_sum",
                    dtype=Int64,
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
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["lower_name", "time"])
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
        source = self._create_file_source(df, schema=schema)

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
                    dtype=Float64,
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
                    dtype=Float64,
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
                    dtype=Float64,
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
                    dtype=Float64,
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

        table = self.flink_table_builder.build(features=features)
        result_df = (
            table.to_pandas().sort_values(by=["name", "time"]).reset_index(drop=True)
        )

        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_first_last_value(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="first_time",
                    dtype=String,
                    transform=OverWindowTransform(
                        expr="`time`",
                        agg_func="FIRST_VALUE",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        limit=2,
                    ),
                ),
                Feature(
                    name="last_time",
                    dtype=String,
                    transform=OverWindowTransform(
                        expr="`time`",
                        agg_func="LAST_VALUE",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        limit=2,
                    ),
                ),
            ],
        )

        expected_df = df.copy()
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

        result_df = (
            self.flink_table_builder.build(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_row_num(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="row_num",
                    dtype=Int64,
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
            self.flink_table_builder.build(feature_view)
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
        source = self._create_file_source(df)

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_value_counts_limit",
                    dtype=MapType(String, Int64),
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
                    dtype=MapType(String, Int64),
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
                {"100": 1},
                {"100": 2},
                {"400": 1},
                {"100": 2},
                {"200": 1, "400": 1},
                {"500": 1},
                {"100": 1, "600": 1},
            ]
        )
        expected_df["cost_value_counts"] = pd.Series(
            [
                {"100": 1},
                {"100": 2},
                {"400": 1},
                {"100": 3},
                {"200": 1, "400": 1},
                {"500": 1},
                {"100": 1, "600": 1},
            ]
        )
        expected_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        table = self.flink_table_builder.build(feature_view)
        result_df = (
            flink_table_to_pandas(table)
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_filter_expr(self):
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
        source = self._create_file_source(df, schema=schema, keys=["name"])

        expected_df = df.copy()
        expected_df["last_2_pay_last_2_minute_total_cost"] = pd.Series(
            [100.0, None, 300.0, None, 400.0, None, 700.0, None, 450.0]
        )
        expected_df.drop(["cost", "action"], axis=1, inplace=True)
        expected_df = expected_df.sort_values(by=["name", "time"]).reset_index(
            drop=True
        )

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="last_2_pay_last_2_minute_total_cost",
                    dtype=Float64,
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        filter_expr="action='pay'",
                        limit=2,
                    ),
                ),
            ],
        )

        table = self.flink_table_builder.build(features=features)
        result_df = (
            table.to_pandas().sort_values(by=["name", "time"]).reset_index(drop=True)
        )

        self.assertTrue(expected_df.equals(result_df))

    def test_over_window_transform_with_different_criteria(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_all_total_cost = Feature(
            name="all_total_cost",
            dtype=Int64,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
            ),
        )
        f_not_ranged_total_cost = Feature(
            name="not_ranged_total_cost",
            dtype=Int64,
            transform=OverWindowTransform(
                expr="cost", agg_func="SUM", group_by_keys=["name"]
            ),
        )
        f_time_window_total_cost = Feature(
            name="time_window_total_cost",
            dtype=Int64,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_row_limit_total_cost = Feature(
            name="row_limit_total_cost",
            dtype=Int64,
            transform=OverWindowTransform(
                expr="cost", agg_func="SUM", group_by_keys=["name"], limit=2
            ),
        )
        f_time_window_row_limit_total_cost = Feature(
            name="time_window_row_limit_total_cost",
            dtype=Int64,
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
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

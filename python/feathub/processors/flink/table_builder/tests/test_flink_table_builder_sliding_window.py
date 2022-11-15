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
import datetime
from datetime import timedelta
from math import sqrt

import pandas as pd

from feathub.common.types import Int64, String, Float64, MapType
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.flink.flink_table import flink_table_to_pandas
from feathub.processors.flink.table_builder.tests.table_builder_test_base import (
    FlinkTableBuilderTestBase,
)
from feathub.table.schema import Schema


class FlinkTableBuilderSlidingWindowTransformTest(FlinkTableBuilderTestBase):
    def test_transform_without_key(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
            ),
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
        )

        expected_result_df = pd.DataFrame(
            [
                [self._to_epoch_millis("2022-01-01 23:59:59.999"), 500],
                [self._to_epoch_millis("2022-01-02 23:59:59.999"), 1000],
                [self._to_epoch_millis("2022-01-03 23:59:59.999"), 1600],
                [self._to_epoch_millis("2022-01-04 23:59:59.999"), 1100],
            ],
            columns=["window_time", "total_cost"],
        )

        result_df = (
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["window_time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_transform_with_limit(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=3),
                group_by_keys=["name"],
                limit=2,
                step_size=timedelta(days=1),
            ),
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
        )

        expected_result_df = pd.DataFrame(
            [
                ["Alex", self._to_epoch_millis("2022-01-01 23:59:59.999"), 100],
                ["Alex", self._to_epoch_millis("2022-01-02 23:59:59.999"), 400],
                ["Alex", self._to_epoch_millis("2022-01-03 23:59:59.999"), 900],
                ["Alex", self._to_epoch_millis("2022-01-04 23:59:59.999"), 900],
                ["Alex", self._to_epoch_millis("2022-01-05 23:59:59.999"), 600],
                ["Emma", self._to_epoch_millis("2022-01-01 23:59:59.999"), 400],
                ["Emma", self._to_epoch_millis("2022-01-02 23:59:59.999"), 600],
                ["Emma", self._to_epoch_millis("2022-01-03 23:59:59.999"), 600],
                ["Emma", self._to_epoch_millis("2022-01-04 23:59:59.999"), 200],
                ["Jack", self._to_epoch_millis("2022-01-03 23:59:59.999"), 500],
                ["Jack", self._to_epoch_millis("2022-01-04 23:59:59.999"), 500],
                ["Jack", self._to_epoch_millis("2022-01-05 23:59:59.999"), 500],
            ],
            columns=["name", "window_time", "total_cost"],
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "window_time"]
        ).reset_index(drop=True)

        result_df = (
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name", "window_time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_transform_with_expression_as_group_by_key(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_name_name = Feature(
            name="name_name", dtype=String, transform="name || '_' || name"
        )

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=3),
                group_by_keys=["name_name"],
                limit=2,
                step_size=timedelta(days=1),
            ),
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_name_name, f_total_cost],
        )

        expected_result_df = pd.DataFrame(
            [
                [self._to_epoch_millis("2022-01-01 23:59:59.999"), "Alex_Alex", 100],
                [self._to_epoch_millis("2022-01-02 23:59:59.999"), "Alex_Alex", 400],
                [self._to_epoch_millis("2022-01-03 23:59:59.999"), "Alex_Alex", 900],
                [self._to_epoch_millis("2022-01-04 23:59:59.999"), "Alex_Alex", 900],
                [self._to_epoch_millis("2022-01-05 23:59:59.999"), "Alex_Alex", 600],
                [self._to_epoch_millis("2022-01-01 23:59:59.999"), "Emma_Emma", 400],
                [self._to_epoch_millis("2022-01-02 23:59:59.999"), "Emma_Emma", 600],
                [self._to_epoch_millis("2022-01-03 23:59:59.999"), "Emma_Emma", 600],
                [self._to_epoch_millis("2022-01-04 23:59:59.999"), "Emma_Emma", 200],
                [self._to_epoch_millis("2022-01-03 23:59:59.999"), "Jack_Jack", 500],
                [self._to_epoch_millis("2022-01-04 23:59:59.999"), "Jack_Jack", 500],
                [self._to_epoch_millis("2022-01-05 23:59:59.999"), "Jack_Jack", 500],
            ],
            columns=["window_time", "name_name", "total_cost"],
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name_name", "window_time"]
        ).reset_index(drop=True)

        result_df = (
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name_name", "window_time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_transform_with_filter_expr(self):
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

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="last_2_minute_total_pay",
                    dtype=Float64,
                    transform=SlidingWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        filter_expr="action='pay'",
                        step_size=timedelta(minutes=1),
                    ),
                ),
                Feature(
                    name="last_2_minute_total_receive",
                    dtype=Float64,
                    transform=SlidingWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        filter_expr="action='receive'",
                        step_size=timedelta(minutes=1),
                    ),
                ),
                Feature(
                    name="pay_count",
                    dtype=Int64,
                    transform=SlidingWindowTransform(
                        expr="0",
                        agg_func="COUNT",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        filter_expr="action='pay'",
                        step_size=timedelta(minutes=1),
                    ),
                ),
            ],
        )

        expected_result_df = pd.DataFrame(
            [
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:01:59.999"),
                    300.0,
                    300.0,
                    2,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:02:59.999"),
                    300.0,
                    300.0,
                    2,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:03:59.999"),
                    0.0,
                    200.0,
                    0,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:04:59.999"),
                    0.0,
                    200.0,
                    0,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:06:59.999"),
                    450.0,
                    0.0,
                    1,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:07:59.999"),
                    450.0,
                    0.0,
                    1,
                ],
                [
                    "Emma",
                    self._to_epoch_millis("2022-01-01 09:02:59.999"),
                    400.0,
                    500.0,
                    1,
                ],
                [
                    "Emma",
                    self._to_epoch_millis("2022-01-01 09:03:59.999"),
                    400.0,
                    500.0,
                    1,
                ],
                [
                    "Emma",
                    self._to_epoch_millis("2022-01-01 09:04:59.999"),
                    300.0,
                    0.0,
                    1,
                ],
                [
                    "Emma",
                    self._to_epoch_millis("2022-01-01 09:05:59.999"),
                    300.0,
                    0.0,
                    1,
                ],
                [
                    "Jack",
                    self._to_epoch_millis("2022-01-01 09:05:59.999"),
                    0.0,
                    500.0,
                    0,
                ],
                [
                    "Jack",
                    self._to_epoch_millis("2022-01-01 09:06:59.999"),
                    0.0,
                    500.0,
                    0,
                ],
            ],
            columns=[
                "name",
                "window_time",
                "last_2_minute_total_pay",
                "last_2_minute_total_receive",
                "pay_count",
            ],
        )

        expected_result_df = expected_result_df.sort_values(
            by=["name", "window_time"]
        ).reset_index(drop=True)

        result_df = (
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name", "window_time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_transform_with_expr_feature_after_sliding_feature(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        features = SlidingFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="first_time",
                    dtype=String,
                    transform=SlidingWindowTransform(
                        expr="`time`",
                        agg_func="FIRST_VALUE",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        step_size=timedelta(days=1),
                    ),
                ),
                Feature(
                    name="last_time",
                    dtype=String,
                    transform=SlidingWindowTransform(
                        expr="`time`",
                        agg_func="LAST_VALUE",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        step_size=timedelta(days=1),
                    ),
                ),
                Feature(
                    name="total_time",
                    dtype=Float64,
                    transform="(UNIX_TIMESTAMP(last_time) - "
                    "UNIX_TIMESTAMP(first_time))",
                    keys=["name"],
                ),
                Feature(
                    name="cnt",
                    dtype=Int64,
                    transform=SlidingWindowTransform(
                        expr="0",
                        agg_func="COUNT",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                        step_size=timedelta(days=1),
                    ),
                ),
                Feature(
                    name="avg_time_per_trip",
                    dtype=Float64,
                    transform="(UNIX_TIMESTAMP(last_time) - "
                    "UNIX_TIMESTAMP(first_time)) / cnt",
                    keys=["name"],
                ),
            ],
        )

        expected_result_df = pd.DataFrame(
            [
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 23:59:59.999"),
                    "2022-01-01 08:01:00",
                    "2022-01-01 08:01:00",
                    0.0,
                    1,
                    0.0,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-02 23:59:59.999"),
                    "2022-01-01 08:01:00",
                    "2022-01-02 08:03:00",
                    86520.0,
                    2,
                    43260.0,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-03 23:59:59.999"),
                    "2022-01-02 08:03:00",
                    "2022-01-03 08:06:00",
                    86580.0,
                    2,
                    43290.0,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-04 23:59:59.999"),
                    "2022-01-03 08:06:00",
                    "2022-01-03 08:06:00",
                    0.0,
                    1,
                    0.0,
                ],
                [
                    "Emma",
                    self._to_epoch_millis("2022-01-01 23:59:59.999"),
                    "2022-01-01 08:02:00",
                    "2022-01-01 08:02:00",
                    0.0,
                    1,
                    0.0,
                ],
                [
                    "Emma",
                    self._to_epoch_millis("2022-01-02 23:59:59.999"),
                    "2022-01-01 08:02:00",
                    "2022-01-02 08:04:00",
                    86520.0,
                    2,
                    43260.0,
                ],
                [
                    "Emma",
                    self._to_epoch_millis("2022-01-03 23:59:59.999"),
                    "2022-01-02 08:04:00",
                    "2022-01-02 08:04:00",
                    0.0,
                    1,
                    0.0,
                ],
                [
                    "Jack",
                    self._to_epoch_millis("2022-01-03 23:59:59.999"),
                    "2022-01-03 08:05:00",
                    "2022-01-03 08:05:00",
                    0.0,
                    1,
                    0.0,
                ],
                [
                    "Jack",
                    self._to_epoch_millis("2022-01-04 23:59:59.999"),
                    "2022-01-03 08:05:00",
                    "2022-01-03 08:05:00",
                    0.0,
                    1,
                    0.0,
                ],
            ],
            columns=[
                "name",
                "window_time",
                "first_time",
                "last_time",
                "total_time",
                "cnt",
                "avg_time_per_trip",
            ],
        )

        expected_result_df = expected_result_df.sort_values(
            by=["name", "window_time"]
        ).reset_index(drop=True)

        result_df = (
            self.flink_table_builder.build(features=features)
            .to_pandas()
            .sort_values(by=["name", "window_time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_join_sliding_feature(self):
        df = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Alex", 200.0, "2022-01-01 09:01:20"],
                ["Alex", 450.0, "2022-01-01 09:06:00"],
            ],
            columns=["name", "cost", "time"],
        )

        schema = Schema(["name", "cost", "time"], [String, Float64, String])
        source = self._create_file_source(df, schema=schema, keys=["name"])

        df2 = pd.DataFrame(
            [
                ["Alex", "2022-01-01 09:01:00"],
                ["Alex", "2022-01-01 09:02:00"],
                ["Alex", "2022-01-01 09:05:00"],
                ["Alex", "2022-01-01 09:07:00"],
                ["Alex", "2022-01-01 09:09:00"],
            ]
        )
        source2 = self._create_file_source(
            df2, schema=Schema(["name", "time"], [String, String]), keys=["name"]
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="last_2_minute_total_cost",
                    dtype=Float64,
                    transform=SlidingWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        step_size=timedelta(minutes=1),
                    ),
                ),
                Feature(
                    name="cnt",
                    dtype=Int64,
                    transform=SlidingWindowTransform(
                        expr="1",
                        agg_func="COUNT",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        step_size=timedelta(minutes=1),
                    ),
                ),
            ],
        )

        joined_feature = DerivedFeatureView(
            name="joined_feature",
            source=source2,
            features=["features.last_2_minute_total_cost", "features.cnt"],
        )
        self.registry.build_features([features])

        built_joined_feature = self.registry.build_features([joined_feature])[0]

        expected_result_df = pd.DataFrame(
            [
                ["Alex", "2022-01-01 09:01:00", 0.0, 0],
                ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                ["Alex", "2022-01-01 09:09:00", 0.0, 0],
            ],
            columns=["name", "time", "last_2_minute_total_cost", "cnt"],
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.flink_table_builder.build(features=built_joined_feature)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_transform_with_value_counts(self):
        df = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Alex", 100.0, "2022-01-01 09:01:20"],
                ["Alex", 200.0, "2022-01-01 09:02:00"],
                ["Alex", 200.0, "2022-01-01 09:02:30"],
            ],
            columns=["name", "cost", "time"],
        )

        schema = Schema(["name", "cost", "time"], [String, Float64, String])
        source = self._create_file_source(df, schema=schema, keys=["name"])

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="last_2_minute_cost_value_counts",
                    dtype=MapType(String, Int64),
                    transform=SlidingWindowTransform(
                        expr="cost",
                        agg_func="VALUE_COUNTS",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        step_size=timedelta(minutes=1),
                        limit=3,
                    ),
                ),
                Feature(
                    name="cnt",
                    dtype=Int64,
                    transform=SlidingWindowTransform(
                        expr="1",
                        agg_func="COUNT",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        step_size=timedelta(minutes=1),
                        limit=3,
                    ),
                ),
            ],
        )

        expected_result_df = pd.DataFrame(
            [
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:01:59.999"),
                    {"100.0": 2},
                    2,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:02:59.999"),
                    {"200.0": 2, "100.0": 1},
                    3,
                ],
                [
                    "Alex",
                    self._to_epoch_millis("2022-01-01 09:03:59.999"),
                    {"200.0": 2},
                    2,
                ],
            ],
            columns=["name", "window_time", "last_2_minute_cost_value_counts", "cnt"],
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "window_time"]
        ).reset_index(drop=True)

        table = self.flink_table_builder.build(features)
        result_df = (
            flink_table_to_pandas(table)
            .sort_values(by=["name", "window_time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_sliding_window_with_millisecond_sliding_window_timestamp(self):
        df = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Alex", 100.0, "2022-01-01 09:01:20"],
                ["Alex", 200.0, "2022-01-01 09:02:00"],
                ["Alex", 200.0, "2022-01-01 09:02:30"],
            ],
            columns=["name", "cost", "time"],
        )

        schema = Schema(["name", "cost", "time"], [String, Float64, String])
        source = self._create_file_source(
            df, schema=schema, keys=["name"], timestamp_format="%Y-%m-%d %H:%M:%S"
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="cnt",
                    dtype=Int64,
                    transform=SlidingWindowTransform(
                        expr="1",
                        agg_func="COUNT",
                        group_by_keys=["name"],
                        window_size=timedelta(minutes=2),
                        step_size=timedelta(minutes=1),
                        limit=3,
                    ),
                ),
                Feature(
                    name="epoch_window_time",
                    dtype=Int64,
                    transform="UNIX_TIMESTAMP(sliding_window_timestamp)",
                ),
            ],
            timestamp_field="sliding_window_timestamp",
            timestamp_format="%Y-%m-%d %H:%M:%S.%f",
        )

        expected_result_df = pd.DataFrame(
            [
                [
                    "Alex",
                    "2022-01-01 09:01:59.999",
                    2,
                    self._to_epoch("2022-01-01 09:01:59.999"),
                ],
                [
                    "Alex",
                    "2022-01-01 09:02:59.999",
                    3,
                    self._to_epoch("2022-01-01 09:02:59.999"),
                ],
                [
                    "Alex",
                    "2022-01-01 09:03:59.999",
                    2,
                    self._to_epoch("2022-01-01 09:03:59.999"),
                ],
            ],
            columns=["name", "sliding_window_timestamp", "cnt", "epoch_window_time"],
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "sliding_window_timestamp"]
        ).reset_index(drop=True)

        table = self.flink_table_builder.build(features)
        result_df = (
            flink_table_to_pandas(table)
            .sort_values(by=["name", "sliding_window_timestamp"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_with_python_udf(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        def name_to_lower(row: pd.Series) -> str:
            return row["name"].lower()

        f_lower_name = Feature(
            name="lower_name", dtype=String, transform=PythonUdfTransform(name_to_lower)
        )

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=3),
                group_by_keys=["lower_name"],
                step_size=timedelta(days=1),
            ),
        )

        f_total_cost_sqrt = Feature(
            name="total_cost_sqrt",
            dtype=Float64,
            transform=PythonUdfTransform(lambda row: sqrt(row["total_cost"])),
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_lower_name, f_total_cost, f_total_cost_sqrt],
        )

        expected_result_df = pd.DataFrame(
            [
                [
                    self._to_epoch_millis("2022-01-01 23:59:59.999"),
                    "alex",
                    100,
                    sqrt(100),
                ],
                [
                    self._to_epoch_millis("2022-01-02 23:59:59.999"),
                    "alex",
                    400,
                    sqrt(400),
                ],
                [
                    self._to_epoch_millis("2022-01-03 23:59:59.999"),
                    "alex",
                    1000,
                    sqrt(1000),
                ],
                [
                    self._to_epoch_millis("2022-01-04 23:59:59.999"),
                    "alex",
                    900,
                    sqrt(900),
                ],
                [
                    self._to_epoch_millis("2022-01-05 23:59:59.999"),
                    "alex",
                    600,
                    sqrt(600),
                ],
                [
                    self._to_epoch_millis("2022-01-01 23:59:59.999"),
                    "emma",
                    400,
                    sqrt(400),
                ],
                [
                    self._to_epoch_millis("2022-01-02 23:59:59.999"),
                    "emma",
                    600,
                    sqrt(600),
                ],
                [
                    self._to_epoch_millis("2022-01-03 23:59:59.999"),
                    "emma",
                    600,
                    sqrt(600),
                ],
                [
                    self._to_epoch_millis("2022-01-04 23:59:59.999"),
                    "emma",
                    200,
                    sqrt(200),
                ],
                [
                    self._to_epoch_millis("2022-01-03 23:59:59.999"),
                    "jack",
                    500,
                    sqrt(500),
                ],
                [
                    self._to_epoch_millis("2022-01-04 23:59:59.999"),
                    "jack",
                    500,
                    sqrt(500),
                ],
                [
                    self._to_epoch_millis("2022-01-05 23:59:59.999"),
                    "jack",
                    500,
                    sqrt(500),
                ],
            ],
            columns=["window_time", "lower_name", "total_cost", "total_cost_sqrt"],
        )
        expected_result_df = expected_result_df.sort_values(
            by=["lower_name", "window_time"]
        ).reset_index(drop=True)

        table = self.flink_table_builder.build(features)
        result_df = (
            flink_table_to_pandas(table)
            .sort_values(by=["lower_name", "window_time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    @staticmethod
    def _to_epoch_millis(
        timestamp_str: str, timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f"
    ) -> int:
        return int(
            datetime.datetime.strptime(timestamp_str, timestamp_format).timestamp()
            * 1000
        )

    @staticmethod
    def _to_epoch(
        timestamp_str: str, timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f"
    ) -> int:
        return int(
            datetime.datetime.strptime(timestamp_str, timestamp_format).timestamp()
        )

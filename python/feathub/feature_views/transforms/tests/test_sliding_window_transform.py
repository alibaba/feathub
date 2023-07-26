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
import time
from abc import ABC
from datetime import timedelta
from enum import Enum
from math import sqrt
from typing import List

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.common.test_utils import to_epoch_millis, to_epoch
from feathub.common.types import Int64, String, Float64, Float32
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class SlidingWindowTestConfig(Enum):
    ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT = {
        ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: True,
        SKIP_SAME_WINDOW_OUTPUT_CONFIG: True,
    }
    DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT = {
        ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: False,
        SKIP_SAME_WINDOW_OUTPUT_CONFIG: False,
    }
    ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT = {
        ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: True,
        SKIP_SAME_WINDOW_OUTPUT_CONFIG: False,
    }


ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT = (
    SlidingWindowTestConfig.ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT
)
DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT = (
    SlidingWindowTestConfig.DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT
)
ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT = (
    SlidingWindowTestConfig.ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT
)


class SlidingWindowTransformITTest(ABC, FeathubITTestBase):
    def get_supported_sliding_window_config(self) -> List[SlidingWindowTestConfig]:
        return [
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT,
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
        ]

    def test_transform_without_key(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
            ),
        )

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [to_epoch_millis("2022-01-01 23:59:59.999"), 500],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), 1000],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), 1600],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), 1100],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), 0],
                ],
                columns=["window_time", "total_cost"],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [to_epoch_millis("2022-01-01 23:59:59.999"), 500],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), 1000],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), 1600],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), 1100],
                ],
                columns=["window_time", "total_cost"],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [to_epoch_millis("2022-01-01 23:59:59.999"), 500],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), 1000],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), 1600],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), 1100],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), 0],
                ],
                columns=["window_time", "total_cost"],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results[props]
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[f_total_cost],
                extra_props=props.value,
            )

            result_df = (
                self.client.get_features(feature_descriptor=features)
                .to_pandas()
                .sort_values(by=["window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_transform_with_limit(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=3),
                group_by_keys=["name"],
                limit=2,
                step_size=timedelta(days=1),
            ),
        )

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", to_epoch_millis("2022-01-01 23:59:59.999"), 100],
                    ["Alex", to_epoch_millis("2022-01-02 23:59:59.999"), 400],
                    ["Alex", to_epoch_millis("2022-01-03 23:59:59.999"), 900],
                    ["Alex", to_epoch_millis("2022-01-05 23:59:59.999"), 600],
                    ["Alex", to_epoch_millis("2022-01-06 23:59:59.999"), 0],
                    ["Emma", to_epoch_millis("2022-01-01 23:59:59.999"), 400],
                    ["Emma", to_epoch_millis("2022-01-02 23:59:59.999"), 600],
                    ["Emma", to_epoch_millis("2022-01-04 23:59:59.999"), 200],
                    ["Emma", to_epoch_millis("2022-01-05 23:59:59.999"), 0],
                    ["Jack", to_epoch_millis("2022-01-03 23:59:59.999"), 500],
                    ["Jack", to_epoch_millis("2022-01-06 23:59:59.999"), 0],
                ],
                columns=["name", "window_time", "total_cost"],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", to_epoch_millis("2022-01-01 23:59:59.999"), 100],
                    ["Alex", to_epoch_millis("2022-01-02 23:59:59.999"), 400],
                    ["Alex", to_epoch_millis("2022-01-03 23:59:59.999"), 900],
                    ["Alex", to_epoch_millis("2022-01-04 23:59:59.999"), 900],
                    ["Alex", to_epoch_millis("2022-01-05 23:59:59.999"), 600],
                    ["Emma", to_epoch_millis("2022-01-01 23:59:59.999"), 400],
                    ["Emma", to_epoch_millis("2022-01-02 23:59:59.999"), 600],
                    ["Emma", to_epoch_millis("2022-01-03 23:59:59.999"), 600],
                    ["Emma", to_epoch_millis("2022-01-04 23:59:59.999"), 200],
                    ["Jack", to_epoch_millis("2022-01-03 23:59:59.999"), 500],
                    ["Jack", to_epoch_millis("2022-01-04 23:59:59.999"), 500],
                    ["Jack", to_epoch_millis("2022-01-05 23:59:59.999"), 500],
                ],
                columns=["name", "window_time", "total_cost"],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", to_epoch_millis("2022-01-01 23:59:59.999"), 100],
                    ["Alex", to_epoch_millis("2022-01-02 23:59:59.999"), 400],
                    ["Alex", to_epoch_millis("2022-01-03 23:59:59.999"), 900],
                    ["Alex", to_epoch_millis("2022-01-04 23:59:59.999"), 900],
                    ["Alex", to_epoch_millis("2022-01-05 23:59:59.999"), 600],
                    ["Alex", to_epoch_millis("2022-01-06 23:59:59.999"), 0],
                    ["Emma", to_epoch_millis("2022-01-01 23:59:59.999"), 400],
                    ["Emma", to_epoch_millis("2022-01-02 23:59:59.999"), 600],
                    ["Emma", to_epoch_millis("2022-01-03 23:59:59.999"), 600],
                    ["Emma", to_epoch_millis("2022-01-04 23:59:59.999"), 200],
                    ["Emma", to_epoch_millis("2022-01-05 23:59:59.999"), 0],
                    ["Jack", to_epoch_millis("2022-01-03 23:59:59.999"), 500],
                    ["Jack", to_epoch_millis("2022-01-04 23:59:59.999"), 500],
                    ["Jack", to_epoch_millis("2022-01-05 23:59:59.999"), 500],
                    ["Jack", to_epoch_millis("2022-01-06 23:59:59.999"), 0],
                ],
                columns=["name", "window_time", "total_cost"],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[f_total_cost],
                extra_props=props.value,
            )
            expected_result_df = expected_result_df.sort_values(
                by=["name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(feature_descriptor=features)
                .to_pandas()
                .sort_values(by=["name", "window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_transform_with_expression_as_group_by_key(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        def repeat_name(row: pd.Series) -> str:
            return row["name"] + "_" + row["name"]

        f_name_name = Feature(
            name="name_name", dtype=String, transform=PythonUdfTransform(repeat_name)
        )

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=3),
                group_by_keys=["name_name"],
                limit=2,
                step_size=timedelta(days=1),
            ),
        )

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [to_epoch_millis("2022-01-01 23:59:59.999"), "Alex_Alex", 100],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), "Alex_Alex", 400],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Alex_Alex", 900],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "Alex_Alex", 600],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "Alex_Alex", 0],
                    [to_epoch_millis("2022-01-01 23:59:59.999"), "Emma_Emma", 400],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), "Emma_Emma", 600],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), "Emma_Emma", 200],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "Emma_Emma", 0],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Jack_Jack", 500],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "Jack_Jack", 0],
                ],
                columns=["window_time", "name_name", "total_cost"],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [to_epoch_millis("2022-01-01 23:59:59.999"), "Alex_Alex", 100],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), "Alex_Alex", 400],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Alex_Alex", 900],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), "Alex_Alex", 900],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "Alex_Alex", 600],
                    [to_epoch_millis("2022-01-01 23:59:59.999"), "Emma_Emma", 400],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), "Emma_Emma", 600],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Emma_Emma", 600],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), "Emma_Emma", 200],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Jack_Jack", 500],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), "Jack_Jack", 500],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "Jack_Jack", 500],
                ],
                columns=["window_time", "name_name", "total_cost"],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [to_epoch_millis("2022-01-01 23:59:59.999"), "Alex_Alex", 100],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), "Alex_Alex", 400],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Alex_Alex", 900],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), "Alex_Alex", 900],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "Alex_Alex", 600],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "Alex_Alex", 0],
                    [to_epoch_millis("2022-01-01 23:59:59.999"), "Emma_Emma", 400],
                    [to_epoch_millis("2022-01-02 23:59:59.999"), "Emma_Emma", 600],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Emma_Emma", 600],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), "Emma_Emma", 200],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "Emma_Emma", 0],
                    [to_epoch_millis("2022-01-03 23:59:59.999"), "Jack_Jack", 500],
                    [to_epoch_millis("2022-01-04 23:59:59.999"), "Jack_Jack", 500],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "Jack_Jack", 500],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "Jack_Jack", 0],
                ],
                columns=["window_time", "name_name", "total_cost"],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[f_name_name, f_total_cost],
                extra_props=props.value,
            )
            expected_result_df = expected_result_df.sort_values(
                by=["name_name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(feature_descriptor=features)
                .to_pandas()
                .sort_values(by=["name_name", "window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

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
        source = self.create_file_source(df, schema=schema, keys=["name"])

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        300.0,
                        300.0,
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        0.0,
                        200.0,
                        0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:05:59.999"),
                        0.0,
                        0.0,
                        0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:06:59.999"),
                        450.0,
                        0.0,
                        1,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:08:59.999"),
                        0.0,
                        0.0,
                        0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        400.0,
                        500.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:04:59.999"),
                        300.0,
                        0.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:06:59.999"),
                        0.0,
                        0.0,
                        0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-01 09:05:59.999"),
                        0.0,
                        500.0,
                        0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-01 09:07:59.999"),
                        0.0,
                        0.0,
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
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        300.0,
                        300.0,
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        300.0,
                        300.0,
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        0.0,
                        200.0,
                        0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:04:59.999"),
                        0.0,
                        200.0,
                        0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:06:59.999"),
                        450.0,
                        0.0,
                        1,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:07:59.999"),
                        450.0,
                        0.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        400.0,
                        500.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        400.0,
                        500.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:04:59.999"),
                        300.0,
                        0.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:05:59.999"),
                        300.0,
                        0.0,
                        1,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-01 09:05:59.999"),
                        0.0,
                        500.0,
                        0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-01 09:06:59.999"),
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
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        300.0,
                        300.0,
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        300.0,
                        300.0,
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        0.0,
                        200.0,
                        0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:04:59.999"),
                        0.0,
                        200.0,
                        0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:05:59.999"),
                        0.0,
                        0.0,
                        0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:06:59.999"),
                        450.0,
                        0.0,
                        1,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:07:59.999"),
                        450.0,
                        0.0,
                        1,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:08:59.999"),
                        0.0,
                        0.0,
                        0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        400.0,
                        500.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        400.0,
                        500.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:04:59.999"),
                        300.0,
                        0.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:05:59.999"),
                        300.0,
                        0.0,
                        1,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 09:06:59.999"),
                        0.0,
                        0.0,
                        0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-01 09:05:59.999"),
                        0.0,
                        500.0,
                        0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-01 09:06:59.999"),
                        0.0,
                        500.0,
                        0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-01 09:07:59.999"),
                        0.0,
                        0.0,
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
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[
                    Feature(
                        name="last_2_minute_total_pay",
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
                        dtype=Float32,
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
                extra_props=props.value,
            )

            expected_result_df = expected_result_df.astype(
                {
                    "name": str,
                    "window_time": "int64",
                    "last_2_minute_total_pay": "float64",
                    "last_2_minute_total_receive": "float32",
                    "pay_count": "int64",
                }
            )

            expected_result_df = expected_result_df.sort_values(
                by=["name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(feature_descriptor=features)
                .to_pandas()
                .sort_values(by=["name", "window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_transform_with_expr_feature_after_sliding_feature(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "2022-01-01 08:01:00",
                        "2022-01-01 08:01:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "2022-01-01 08:01:00",
                        "2022-01-02 08:03:00",
                        86520.0,
                        2,
                        43260.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-02 08:03:00",
                        "2022-01-03 08:06:00",
                        86580.0,
                        2,
                        43290.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "2022-01-03 08:06:00",
                        "2022-01-03 08:06:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        None,
                        None,
                        None,
                        0,
                        None,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "2022-01-01 08:02:00",
                        "2022-01-01 08:02:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "2022-01-01 08:02:00",
                        "2022-01-02 08:04:00",
                        86520.0,
                        2,
                        43260.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-02 08:04:00",
                        "2022-01-02 08:04:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        None,
                        None,
                        None,
                        0,
                        None,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-03 08:05:00",
                        "2022-01-03 08:05:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        None,
                        None,
                        None,
                        0,
                        None,
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
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "2022-01-01 08:01:00",
                        "2022-01-01 08:01:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "2022-01-01 08:01:00",
                        "2022-01-02 08:03:00",
                        86520.0,
                        2,
                        43260.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-02 08:03:00",
                        "2022-01-03 08:06:00",
                        86580.0,
                        2,
                        43290.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "2022-01-03 08:06:00",
                        "2022-01-03 08:06:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "2022-01-01 08:02:00",
                        "2022-01-01 08:02:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "2022-01-01 08:02:00",
                        "2022-01-02 08:04:00",
                        86520.0,
                        2,
                        43260.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-02 08:04:00",
                        "2022-01-02 08:04:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-03 08:05:00",
                        "2022-01-03 08:05:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-04 23:59:59.999"),
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
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "2022-01-01 08:01:00",
                        "2022-01-01 08:01:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "2022-01-01 08:01:00",
                        "2022-01-02 08:03:00",
                        86520.0,
                        2,
                        43260.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-02 08:03:00",
                        "2022-01-03 08:06:00",
                        86580.0,
                        2,
                        43290.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "2022-01-03 08:06:00",
                        "2022-01-03 08:06:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        None,
                        None,
                        None,
                        0,
                        None,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "2022-01-01 08:02:00",
                        "2022-01-01 08:02:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "2022-01-01 08:02:00",
                        "2022-01-02 08:04:00",
                        86520.0,
                        2,
                        43260.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-02 08:04:00",
                        "2022-01-02 08:04:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Emma",
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        None,
                        None,
                        None,
                        0,
                        None,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "2022-01-03 08:05:00",
                        "2022-01-03 08:05:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "2022-01-03 08:05:00",
                        "2022-01-03 08:05:00",
                        0.0,
                        1,
                        0.0,
                    ],
                    [
                        "Jack",
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        None,
                        None,
                        None,
                        0,
                        None,
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
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="feature_view",
                source=source,
                features=[
                    Feature(
                        name="first_time",
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
                extra_props=props.value,
            )

            expected_result_df = expected_result_df.sort_values(
                by=["name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(feature_descriptor=features)
                .to_pandas()
                .sort_values(by=["name", "window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

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
        source = self.create_file_source(
            df, schema=schema, keys=["name"], name="source"
        )

        df2 = pd.DataFrame(
            [
                ["Alex", "2022-01-01 09:01:00"],
                ["Alex", "2022-01-01 09:02:00"],
                ["Alex", "2022-01-01 09:05:00"],
                ["Alex", "2022-01-01 09:07:00"],
                ["Alex", "2022-01-01 09:09:00"],
            ]
        )
        source2 = self.create_file_source(
            df2,
            schema=Schema(["name", "time"], [String, String]),
            keys=["name"],
            name="source2",
        )

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", "2022-01-01 09:01:00", None, None],
                    ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                    ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                    ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                ],
                columns=["name", "time", "last_2_minute_total_cost", "cnt"],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", "2022-01-01 09:01:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                    ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                    ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                ],
                columns=["name", "time", "last_2_minute_total_cost", "cnt"],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", "2022-01-01 09:01:00", None, None],
                    ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                    ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                    ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                ],
                columns=["name", "time", "last_2_minute_total_cost", "cnt"],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[
                    Feature(
                        name="last_2_minute_total_cost",
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
                        transform=SlidingWindowTransform(
                            expr="1",
                            agg_func="COUNT",
                            group_by_keys=["name"],
                            window_size=timedelta(minutes=2),
                            step_size=timedelta(minutes=1),
                        ),
                    ),
                ],
                extra_props=props.value,
            )

            joined_feature = DerivedFeatureView(
                name="joined_feature",
                source=source2,
                features=["features.last_2_minute_total_cost", "features.cnt"],
                keep_source_fields=True,
            )
            self.client.build_features([features])

            built_joined_feature = self.client.build_features([joined_feature])[0]

            expected_result_df = expected_result_df.sort_values(
                by=["name", "time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(feature_descriptor=built_joined_feature)
                .to_pandas()
                .sort_values(by=["name", "time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_join_sliding_feature_with_different_filter(self):
        df = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Alex", 200.0, "2022-01-01 09:01:20"],
                ["Alex", 450.0, "2022-01-01 09:06:00"],
            ],
            columns=["name", "cost", "time"],
        )

        schema = Schema(["name", "cost", "time"], [String, Float64, String])
        source = self.create_file_source(
            df, schema=schema, keys=["name"], name="source"
        )

        df2 = pd.DataFrame(
            [
                ["Alex", "2022-01-01 09:01:00"],
                ["Alex", "2022-01-01 09:02:00"],
                ["Alex", "2022-01-01 09:05:00"],
                ["Alex", "2022-01-01 09:07:00"],
                ["Alex", "2022-01-01 09:09:00"],
            ]
        )
        source2 = self.create_file_source(
            df2,
            schema=Schema(["name", "time"], [String, String]),
            keys=["name"],
            name="source2",
        )

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", "2022-01-01 09:01:00", None, None],
                    ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                    ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                    ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                ],
                columns=["name", "time", "last_2_minute_total_cost", "cnt"],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", "2022-01-01 09:01:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                    ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                    ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                ],
                columns=["name", "time", "last_2_minute_total_cost", "cnt"],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", "2022-01-01 09:01:00", None, None],
                    ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                    ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                    ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                    ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                ],
                columns=["name", "time", "last_2_minute_total_cost", "cnt"],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[
                    Feature(
                        name="last_2_minute_total_cost",
                        transform=SlidingWindowTransform(
                            expr="cost",
                            filter_expr="cost >= 0",
                            agg_func="SUM",
                            group_by_keys=["name"],
                            window_size=timedelta(minutes=2),
                            step_size=timedelta(minutes=1),
                        ),
                    ),
                    Feature(
                        name="cnt",
                        transform=SlidingWindowTransform(
                            expr="1",
                            agg_func="COUNT",
                            group_by_keys=["name"],
                            window_size=timedelta(minutes=2),
                            step_size=timedelta(minutes=1),
                        ),
                    ),
                ],
                extra_props=props.value,
            )

            joined_feature = DerivedFeatureView(
                name="joined_feature",
                source=source2,
                features=[
                    "features.last_2_minute_total_cost",
                    "features.cnt",
                ],
                keep_source_fields=True,
            )
            self.client.build_features([features])

            built_joined_feature = self.client.build_features([joined_feature])[0]

            expected_result_df = expected_result_df.sort_values(
                by=["name", "time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(feature_descriptor=built_joined_feature)
                .to_pandas()
                .sort_values(by=["name", "time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_sliding_feature_join_source(self):
        df = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Alex", 200.0, "2022-01-01 09:01:20"],
                ["Alex", 450.0, "2022-01-01 09:06:00"],
            ],
            columns=["name", "cost", "time"],
        )

        schema = Schema(["name", "cost", "time"], [String, Float64, String])
        source = self.create_file_source(
            df, schema=schema, keys=["name"], name="source"
        )

        df2 = pd.DataFrame(
            [
                ["Alex", "a", "2022-01-01 09:01:00"],
                ["Alex", "b", "2022-01-01 09:02:00"],
                ["Alex", "c", "2022-01-01 09:05:00"],
                ["Alex", "d", "2022-01-01 09:07:00"],
                ["Alex", "e", "2022-01-01 09:09:00"],
            ]
        )
        source2 = self.create_file_source(
            df2,
            schema=Schema(["name", "feature", "time"], [String, String, String]),
            keys=["name"],
            name="source2",
            timestamp_field="time",
        )

        sliding_features = SlidingFeatureView(
            name="sliding_features",
            source=source,
            features=[
                Feature(
                    name="last_2_minute_total_cost",
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

        joined_features = DerivedFeatureView(
            name="joined_feature",
            source=sliding_features,
            features=["source2.feature"],
            keep_source_fields=True,
        )

        built_joined_feature = self.client.build_features([source2, joined_features])[1]

        expected_result_df = pd.DataFrame(
            [
                ["Alex", to_epoch_millis("2022-01-01 09:01:59.999"), 300.0, 2, "a"],
                ["Alex", to_epoch_millis("2022-01-01 09:03:59.999"), 0.0, 0, "b"],
                ["Alex", to_epoch_millis("2022-01-01 09:06:59.999"), 450.0, 1, "c"],
                ["Alex", to_epoch_millis("2022-01-01 09:08:59.999"), 0.0, 0, "d"],
            ],
            columns=[
                "name",
                "window_time",
                "last_2_minute_total_cost",
                "cnt",
                "feature",
            ],
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "window_time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=built_joined_feature)
            .to_pandas()
            .sort_values(by=["name", "window_time"])
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
        source = self.create_file_source(df, schema=schema, keys=["name"])

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        {100.0: 2},
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        {200.0: 2, 100.0: 1},
                        3,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        {200.0: 2},
                        2,
                    ],
                    ["Alex", to_epoch_millis("2022-01-01 09:04:59.999"), None, 0],
                ],
                columns=[
                    "name",
                    "window_time",
                    "last_2_minute_cost_value_counts",
                    "cnt",
                ],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        {100.0: 2},
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        {200.0: 2, 100.0: 1},
                        3,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        {200.0: 2},
                        2,
                    ],
                ],
                columns=[
                    "name",
                    "window_time",
                    "last_2_minute_cost_value_counts",
                    "cnt",
                ],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        {100.0: 2},
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        {200.0: 2, 100.0: 1},
                        3,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        {200.0: 2},
                        2,
                    ],
                    ["Alex", to_epoch_millis("2022-01-01 09:04:59.999"), None, 0],
                ],
                columns=[
                    "name",
                    "window_time",
                    "last_2_minute_cost_value_counts",
                    "cnt",
                ],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[
                    Feature(
                        name="last_2_minute_cost_value_counts",
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
                extra_props=props.value,
            )

            expected_result_df = expected_result_df.sort_values(
                by=["name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(features)
                .to_pandas()
                .sort_values(by=["name", "window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_transform_with_collect_list(self):
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
        source = self.create_file_source(df, schema=schema, keys=["name"])

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        [100.0, 100.0],
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        [100.0, 200.0, 200.0],
                        3,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        [200.0, 200.0],
                        2,
                    ],
                    ["Alex", to_epoch_millis("2022-01-01 09:04:59.999"), None, 0],
                ],
                columns=[
                    "name",
                    "window_time",
                    "last_2_minute_cost_collect_list",
                    "cnt",
                ],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        [100.0, 100.0],
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        [100.0, 200.0, 200.0],
                        3,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        [200.0, 200.0],
                        2,
                    ],
                ],
                columns=[
                    "name",
                    "window_time",
                    "last_2_minute_cost_collect_list",
                    "cnt",
                ],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:01:59.999"),
                        [100.0, 100.0],
                        2,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:02:59.999"),
                        [100.0, 200.0, 200.0],
                        3,
                    ],
                    [
                        "Alex",
                        to_epoch_millis("2022-01-01 09:03:59.999"),
                        [200.0, 200.0],
                        2,
                    ],
                    ["Alex", to_epoch_millis("2022-01-01 09:04:59.999"), None, 0],
                ],
                columns=[
                    "name",
                    "window_time",
                    "last_2_minute_cost_collect_list",
                    "cnt",
                ],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[
                    Feature(
                        name="last_2_minute_cost_collect_list",
                        transform=SlidingWindowTransform(
                            expr="cost",
                            agg_func="COLLECT_LIST",
                            group_by_keys=["name"],
                            window_size=timedelta(minutes=2),
                            step_size=timedelta(minutes=1),
                            limit=3,
                        ),
                    ),
                    Feature(
                        name="cnt",
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
                extra_props=props.value,
            )

            expected_result_df = expected_result_df.sort_values(
                by=["name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(features)
                .to_pandas()
                .sort_values(by=["name", "window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

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
        source = self.create_file_source(
            df, schema=schema, keys=["name"], timestamp_format="%Y-%m-%d %H:%M:%S"
        )

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        "2022-01-01 09:01:59.999",
                        2,
                        to_epoch("2022-01-01 09:01:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:02:59.999",
                        3,
                        to_epoch("2022-01-01 09:02:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:03:59.999",
                        2,
                        to_epoch("2022-01-01 09:03:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:04:59.999",
                        0,
                        to_epoch("2022-01-01 09:04:59.999"),
                    ],
                ],
                columns=[
                    "name",
                    "sliding_window_timestamp",
                    "cnt",
                    "epoch_window_time",
                ],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        "2022-01-01 09:01:59.999",
                        2,
                        to_epoch("2022-01-01 09:01:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:02:59.999",
                        3,
                        to_epoch("2022-01-01 09:02:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:03:59.999",
                        2,
                        to_epoch("2022-01-01 09:03:59.999"),
                    ],
                ],
                columns=[
                    "name",
                    "sliding_window_timestamp",
                    "cnt",
                    "epoch_window_time",
                ],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        "Alex",
                        "2022-01-01 09:01:59.999",
                        2,
                        to_epoch("2022-01-01 09:01:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:02:59.999",
                        3,
                        to_epoch("2022-01-01 09:02:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:03:59.999",
                        2,
                        to_epoch("2022-01-01 09:03:59.999"),
                    ],
                    [
                        "Alex",
                        "2022-01-01 09:04:59.999",
                        0,
                        to_epoch("2022-01-01 09:04:59.999"),
                    ],
                ],
                columns=[
                    "name",
                    "sliding_window_timestamp",
                    "cnt",
                    "epoch_window_time",
                ],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[
                    Feature(
                        name="cnt",
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
                        transform="UNIX_TIMESTAMP(sliding_window_timestamp, "
                        "'%Y-%m-%d %H:%M:%S.%f')",
                    ),
                ],
                timestamp_field="sliding_window_timestamp",
                timestamp_format="%Y-%m-%d %H:%M:%S.%f",
                extra_props=props.value,
            )

            expected_result_df = expected_result_df.sort_values(
                by=["name", "sliding_window_timestamp"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(features)
                .to_pandas()
                .sort_values(by=["name", "sliding_window_timestamp"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_sliding_window_with_python_udf(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        def name_to_lower(row: pd.Series) -> str:
            return row["name"].lower()

        f_lower_name = Feature(
            name="lower_name",
            dtype=String,
            transform=PythonUdfTransform(name_to_lower),
        )

        f_total_cost = Feature(
            name="total_cost",
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

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "alex",
                        100,
                        sqrt(100),
                    ],
                    [
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "alex",
                        400,
                        sqrt(400),
                    ],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "alex",
                        1000,
                        sqrt(1000),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "alex",
                        900,
                        sqrt(900),
                    ],
                    [
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        "alex",
                        600,
                        sqrt(600),
                    ],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "alex", 0, 0],
                    [
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "emma",
                        400,
                        sqrt(400),
                    ],
                    [
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "emma",
                        600,
                        sqrt(600),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "emma",
                        200,
                        sqrt(200),
                    ],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "emma", 0, 0],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "jack",
                        500,
                        sqrt(500),
                    ],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "jack", 0, 0],
                ],
                columns=[
                    "window_time",
                    "lower_name",
                    "total_cost",
                    "total_cost_sqrt",
                ],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "alex",
                        100,
                        sqrt(100),
                    ],
                    [
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "alex",
                        400,
                        sqrt(400),
                    ],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "alex",
                        1000,
                        sqrt(1000),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "alex",
                        900,
                        sqrt(900),
                    ],
                    [
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        "alex",
                        600,
                        sqrt(600),
                    ],
                    [
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "emma",
                        400,
                        sqrt(400),
                    ],
                    [
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "emma",
                        600,
                        sqrt(600),
                    ],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "emma",
                        600,
                        sqrt(600),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "emma",
                        200,
                        sqrt(200),
                    ],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "jack",
                        500,
                        sqrt(500),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "jack",
                        500,
                        sqrt(500),
                    ],
                    [
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        "jack",
                        500,
                        sqrt(500),
                    ],
                ],
                columns=[
                    "window_time",
                    "lower_name",
                    "total_cost",
                    "total_cost_sqrt",
                ],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    [
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "alex",
                        100,
                        sqrt(100),
                    ],
                    [
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "alex",
                        400,
                        sqrt(400),
                    ],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "alex",
                        1000,
                        sqrt(1000),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "alex",
                        900,
                        sqrt(900),
                    ],
                    [
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        "alex",
                        600,
                        sqrt(600),
                    ],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "alex", 0, 0],
                    [
                        to_epoch_millis("2022-01-01 23:59:59.999"),
                        "emma",
                        400,
                        sqrt(400),
                    ],
                    [
                        to_epoch_millis("2022-01-02 23:59:59.999"),
                        "emma",
                        600,
                        sqrt(600),
                    ],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "emma",
                        600,
                        sqrt(600),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "emma",
                        200,
                        sqrt(200),
                    ],
                    [to_epoch_millis("2022-01-05 23:59:59.999"), "emma", 0, 0],
                    [
                        to_epoch_millis("2022-01-03 23:59:59.999"),
                        "jack",
                        500,
                        sqrt(500),
                    ],
                    [
                        to_epoch_millis("2022-01-04 23:59:59.999"),
                        "jack",
                        500,
                        sqrt(500),
                    ],
                    [
                        to_epoch_millis("2022-01-05 23:59:59.999"),
                        "jack",
                        500,
                        sqrt(500),
                    ],
                    [to_epoch_millis("2022-01-06 23:59:59.999"), "jack", 0, 0],
                ],
                columns=[
                    "window_time",
                    "lower_name",
                    "total_cost",
                    "total_cost_sqrt",
                ],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[f_lower_name, f_total_cost, f_total_cost_sqrt],
                extra_props=props.value,
            )

            expected_result_df = expected_result_df.sort_values(
                by=["lower_name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(features)
                .to_pandas()
                .sort_values(by=["lower_name", "window_time"])
                .reset_index(drop=True)
            )
            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_multiple_window_size_with_same_step(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost_two_days = Feature(
            name="total_cost_two_days",
            dtype=Int64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_avg_cost_three_days = Feature(
            name="avg_cost_three_days",
            dtype=Float32,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="AVG",
                window_size=timedelta(days=3),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_max_cost_two_days = Feature(
            name="max_cost_two_days",
            dtype=Float64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="MAX",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_min_cost_two_days = Feature(
            name="min_cost_two_days",
            dtype=Float64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="MIN",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_first_cost_two_days = Feature(
            name="first_cost_two_days",
            dtype=Float64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="FIRST_VALUE",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_last_cost_two_days = Feature(
            name="last_cost_two_days",
            dtype=Float64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="LAST_VALUE",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_cost_count_two_days = Feature(
            name="cost_count_two_days",
            dtype=Float64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="COUNT",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_cost_value_counts_two_days = Feature(
            name="cost_value_counts_two_days",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="VALUE_COUNTS",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        expected_result_df = pd.DataFrame(
            [
                [
                    "Alex",
                    to_epoch_millis("2022-01-01 23:59:59.999"),
                    100,
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    1.0,
                    {100: 1},
                ],
                [
                    "Alex",
                    to_epoch_millis("2022-01-02 23:59:59.999"),
                    400,
                    200.0,
                    100.0,
                    300.0,
                    100.0,
                    300.0,
                    2.0,
                    {100: 1, 300: 1},
                ],
                [
                    "Alex",
                    to_epoch_millis("2022-01-03 23:59:59.999"),
                    900,
                    1000 / 3,
                    300.0,
                    600.0,
                    300.0,
                    600.0,
                    2.0,
                    {300: 1, 600: 1},
                ],
                [
                    "Alex",
                    to_epoch_millis("2022-01-04 23:59:59.999"),
                    600,
                    450.0,
                    600.0,
                    600.0,
                    600.0,
                    600.0,
                    1.0,
                    {600: 1},
                ],
                [
                    "Alex",
                    to_epoch_millis("2022-01-05 23:59:59.999"),
                    0,
                    600.0,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    None,
                ],
                [
                    "Alex",
                    to_epoch_millis("2022-01-06 23:59:59.999"),
                    0,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    None,
                ],
                [
                    "Emma",
                    to_epoch_millis("2022-01-01 23:59:59.999"),
                    400,
                    400.0,
                    400.0,
                    400.0,
                    400.0,
                    400.0,
                    1.0,
                    {400: 1},
                ],
                [
                    "Emma",
                    to_epoch_millis("2022-01-02 23:59:59.999"),
                    600,
                    300.0,
                    200.0,
                    400.0,
                    400.0,
                    200.0,
                    2.0,
                    {400: 1, 200: 1},
                ],
                [
                    "Emma",
                    to_epoch_millis("2022-01-03 23:59:59.999"),
                    200,
                    300.0,
                    200.0,
                    200.0,
                    200.0,
                    200.0,
                    1.0,
                    {200: 1},
                ],
                [
                    "Emma",
                    to_epoch_millis("2022-01-04 23:59:59.999"),
                    0,
                    200.0,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    None,
                ],
                [
                    "Emma",
                    to_epoch_millis("2022-01-05 23:59:59.999"),
                    0,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    None,
                ],
                [
                    "Jack",
                    to_epoch_millis("2022-01-03 23:59:59.999"),
                    500,
                    500.0,
                    500.0,
                    500.0,
                    500.0,
                    500.0,
                    1.0,
                    {500: 1},
                ],
                [
                    "Jack",
                    to_epoch_millis("2022-01-05 23:59:59.999"),
                    0,
                    500.0,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    None,
                ],
                [
                    "Jack",
                    to_epoch_millis("2022-01-06 23:59:59.999"),
                    0,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    None,
                ],
            ],
            columns=[
                "name",
                "window_time",
                "total_cost_two_days",
                "avg_cost_three_days",
                "min_cost_two_days",
                "max_cost_two_days",
                "first_cost_two_days",
                "last_cost_two_days",
                "cost_count_two_days",
                "cost_value_counts_two_days",
            ],
        )

        expected_result_df = expected_result_df.astype(
            {
                "name": str,
                "window_time": "int64",
                "total_cost_two_days": "int64",
                "avg_cost_three_days": "float32",
                "min_cost_two_days": "float64",
                "max_cost_two_days": "float64",
                "first_cost_two_days": "float64",
                "last_cost_two_days": "float64",
                "cost_count_two_days": "float64",
                "cost_value_counts_two_days": object,
            }
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[
                f_total_cost_two_days,
                f_avg_cost_three_days,
                f_min_cost_two_days,
                f_max_cost_two_days,
                f_first_cost_two_days,
                f_last_cost_two_days,
                f_cost_count_two_days,
                f_cost_value_counts_two_days,
            ],
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["name", "window_time"])
            .reset_index(drop=True)
        )

        self.assertTrue(
            expected_result_df.equals(result_df),
            f"expected: {expected_result_df}\n" f"actual: {result_df}",
        )

    def test_sliding_window_feature_with_dtype(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=1),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
            dtype=Float64,
        )

        expected_results = {
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", to_epoch_millis("2022-01-01 23:59:59.999"), 100.0],
                    ["Alex", to_epoch_millis("2022-01-02 23:59:59.999"), 300.0],
                    ["Alex", to_epoch_millis("2022-01-03 23:59:59.999"), 600.0],
                    ["Alex", to_epoch_millis("2022-01-04 23:59:59.999"), 0.0],
                    ["Emma", to_epoch_millis("2022-01-01 23:59:59.999"), 400.0],
                    ["Emma", to_epoch_millis("2022-01-02 23:59:59.999"), 200.0],
                    ["Emma", to_epoch_millis("2022-01-03 23:59:59.999"), 0.0],
                    ["Jack", to_epoch_millis("2022-01-03 23:59:59.999"), 500.0],
                    ["Jack", to_epoch_millis("2022-01-04 23:59:59.999"), 0.0],
                ],
                columns=["name", "window_time", "total_cost"],
            ),
            DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", to_epoch_millis("2022-01-01 23:59:59.999"), 100.0],
                    ["Alex", to_epoch_millis("2022-01-02 23:59:59.999"), 300.0],
                    ["Alex", to_epoch_millis("2022-01-03 23:59:59.999"), 600.0],
                    ["Emma", to_epoch_millis("2022-01-01 23:59:59.999"), 400.0],
                    ["Emma", to_epoch_millis("2022-01-02 23:59:59.999"), 200.0],
                    ["Jack", to_epoch_millis("2022-01-03 23:59:59.999"), 500.0],
                ],
                columns=["name", "window_time", "total_cost"],
            ),
            ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT: pd.DataFrame(
                [
                    ["Alex", to_epoch_millis("2022-01-01 23:59:59.999"), 100.0],
                    ["Alex", to_epoch_millis("2022-01-02 23:59:59.999"), 300.0],
                    ["Alex", to_epoch_millis("2022-01-03 23:59:59.999"), 600.0],
                    ["Alex", to_epoch_millis("2022-01-04 23:59:59.999"), 0.0],
                    ["Emma", to_epoch_millis("2022-01-01 23:59:59.999"), 400.0],
                    ["Emma", to_epoch_millis("2022-01-02 23:59:59.999"), 200.0],
                    ["Emma", to_epoch_millis("2022-01-03 23:59:59.999"), 0.0],
                    ["Jack", to_epoch_millis("2022-01-03 23:59:59.999"), 500.0],
                    ["Jack", to_epoch_millis("2022-01-04 23:59:59.999"), 0.0],
                ],
                columns=["name", "window_time", "total_cost"],
            ),
        }

        for props in self.get_supported_sliding_window_config():
            expected_result_df = expected_results.get(props)
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[f_total_cost],
                extra_props=props.value,
            )

            result_df = (
                self.client.get_features(feature_descriptor=features)
                .to_pandas()
                .sort_values(by=["name", "window_time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_late_data(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Alex", 200, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 250, "2022-01-02 08:04:00"],
                ["Alex", 400, 200, "2022-01-01 08:03:00"],
                ["Alex", 500, 800, "2022-01-04 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        expected_result = pd.DataFrame(
            [
                [
                    to_epoch_millis("2022-01-01 23:59:59.999"),
                    {"100": 1, "200": 1, "400": 1},
                    {"100": 1, "200": 1, "400": 1},
                ],
                [
                    to_epoch_millis("2022-01-02 23:59:59.999"),
                    {"300": 1},
                    {"100": 1, "200": 1, "300": 1, "400": 1},
                ],
                [to_epoch_millis("2022-01-03 23:59:59.999"), None, {"300": 1}],
                [to_epoch_millis("2022-01-04 23:59:59.999"), {"500": 1}, {"500": 1}],
                [to_epoch_millis("2022-01-05 23:59:59.999"), None, {"500": 1}],
                [to_epoch_millis("2022-01-06 23:59:59.999"), None, None],
            ],
            columns=["window_time", "total_cost", "total_cost_2"],
        )

        self._test_late_data(df, expected_result)

    def test_late_data_at_first_position(self):
        df = pd.DataFrame(
            [
                ["Emmy", 100, 100, "2022-01-02 08:01:00"],
                ["Alex", 200, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 250, "2022-01-02 08:04:00"],
                ["Alex", 400, 200, "2022-01-03 08:03:00"],
                ["Alex", 500, 800, "2022-01-04 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        expected_result = pd.DataFrame(
            [
                [to_epoch_millis("2022-01-01 23:59:59.999"), {"200": 1}, {"200": 1}],
                [
                    to_epoch_millis("2022-01-02 23:59:59.999"),
                    {"300": 1},
                    {"200": 1, "300": 1},
                ],
                [
                    to_epoch_millis("2022-01-03 23:59:59.999"),
                    {"400": 1},
                    {"300": 1, "400": 1},
                ],
                [
                    to_epoch_millis("2022-01-04 23:59:59.999"),
                    {"500": 1},
                    {"400": 1, "500": 1},
                ],
                [to_epoch_millis("2022-01-05 23:59:59.999"), None, {"500": 1}],
                [to_epoch_millis("2022-01-06 23:59:59.999"), None, None],
            ],
            columns=["window_time", "total_cost", "total_cost_2"],
        )

        self._test_late_data(df, expected_result)

    def test_late_data_at_last_position(self):
        df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Alex", 200, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 250, "2022-01-02 08:04:00"],
                ["Alex", 500, 800, "2022-01-04 08:06:00"],
                ["Alex", 400, 200, "2022-01-03 08:03:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        expected_result = pd.DataFrame(
            [
                [
                    to_epoch_millis("2022-01-01 23:59:59.999"),
                    {"100": 1, "200": 1},
                    {"100": 1, "200": 1},
                ],
                [
                    to_epoch_millis("2022-01-02 23:59:59.999"),
                    {"300": 1},
                    {"100": 1, "200": 1, "300": 1},
                ],
                [
                    to_epoch_millis("2022-01-03 23:59:59.999"),
                    {"400": 1},
                    {"300": 1, "400": 1},
                ],
                [
                    to_epoch_millis("2022-01-04 23:59:59.999"),
                    {"500": 1},
                    {"400": 1, "500": 1},
                ],
                [to_epoch_millis("2022-01-05 23:59:59.999"), None, {"500": 1}],
                [to_epoch_millis("2022-01-06 23:59:59.999"), None, None],
            ],
            columns=["window_time", "total_cost", "total_cost_2"],
        )

        self._test_late_data(df, expected_result)

    def _test_late_data(self, df, expected_result):
        client = self.get_client(
            {
                "processor": {
                    "flink": {
                        "native.parallelism.default": "1",
                        "native.pipeline.max-parallelism": "1",
                    },
                },
            }
        )

        schema = (
            Schema.new_builder()
            .column("name", String)
            .column("cost", String)
            .column("distance", Int64)
            .column("time", String)
            .build()
        )

        source = self.create_file_source(df, schema=schema)

        # TODO: When df contains 5 records, the following udf would
        #  be invoked 15 times. Figure out why and fix this bug to
        #  reduce performance overhead.
        def sleep(row: pd.Series) -> str:
            time.sleep(0.1)
            return row["cost"]

        features: FeatureView = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost2",
                    dtype=String,
                    transform=PythonUdfTransform(sleep),
                    keys=["name"],
                )
            ],
            keep_source_fields=True,
        )

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost2",
                agg_func="VALUE_COUNTS",
                window_size=timedelta(days=1),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        f_total_cost_2 = Feature(
            name="total_cost_2",
            transform=SlidingWindowTransform(
                expr="cost2",
                agg_func="VALUE_COUNTS",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
                group_by_keys=["name"],
            ),
        )

        features = SlidingFeatureView(
            name="features",
            source=features,
            features=[f_total_cost, f_total_cost_2],
        )

        result_df = (
            client.get_features(feature_descriptor=features)
            .to_pandas()
            .sort_values(by=["name", "window_time"])
            .reset_index(drop=True)
        )
        result_df = result_df[result_df["name"] == "Alex"]
        result_df = result_df.drop("name", axis=1)

        self.assertTrue(expected_result.equals(result_df))

    def test_transform_with_zero_window_size(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(seconds=0),
                step_size=timedelta(seconds=0),
            ),
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
        )

        with self.assertRaises(FeathubException) as cm:
            self.client.get_features(feature_descriptor=features).to_pandas()
        self.assertIn(
            "Sliding window size 0:00:00 must be a positive value.",
            cm.exception.args[0],
        )

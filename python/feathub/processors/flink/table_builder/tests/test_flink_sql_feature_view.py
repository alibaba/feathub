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
import typing
from abc import ABC
from datetime import timedelta

import pandas as pd

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.test_utils import to_epoch_millis
from feathub.common.types import Int64
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.sql_feature_view import SqlFeatureView
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.flink.flink_processor import FlinkProcessor
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


# TODO: Supports the cases that using a SqlFeatureView as a direct or
#  indirect source of another SqlFeatureView.
class FlinkSqlFeatureViewITTest(ABC, FeathubITTestBase):
    def setUp(self) -> None:
        FeathubITTestBase.setUp(self)
        self.source = self.create_file_source(self.input_data.copy())
        self.client.build_features([self.source])

    def tearDown(self) -> None:
        FeathubITTestBase.tearDown(self)
        t_env = typing.cast(
            FlinkProcessor, self.client.processor
        ).flink_table_builder.t_env
        for view_name in t_env.list_temporary_views():
            t_env.drop_temporary_view(view_name)

    def test_sql_feature_view(self):
        features = SqlFeatureView(
            name="test_view_name",
            sql_statement=f"""
                SELECT name, cost FROM {self.source.name};
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .build()
            ),
        )

        expected_result_df = pd.DataFrame(
            [
                ["Alex", 100],
                ["Emma", 400],
                ["Alex", 300],
                ["Emma", 200],
                ["Jack", 500],
                ["Alex", 600],
            ],
            columns=["name", "cost"],
        )

        self.client.build_features([features])

        result_df = (
            self.client.get_features(feature_descriptor=features)
            .to_pandas()
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_usage_with_keys(self):
        features = SqlFeatureView(
            name="test_view_name",
            sql_statement=f"""
                SELECT name, cost FROM {self.source.name};
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .build()
            ),
            keys=["name"],
        )

        keys = pd.DataFrame(
            [
                ["Alex"],
                ["Jack"],
                ["Dummy"],
            ],
            columns=["name"],
        )

        expected_result_df = pd.DataFrame(
            [
                ["Alex", 100],
                ["Alex", 300],
                ["Alex", 600],
                ["Jack", 500],
            ],
            columns=["name", "cost"],
        )

        self.client.build_features([features])

        result_df = (
            self.client.get_features(feature_descriptor=features, keys=keys)
            .to_pandas()
            .sort_values(by=["name", "cost"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_usage_with_udf(self):
        features = SqlFeatureView(
            name="test_view_name",
            sql_statement=f"""
                SELECT *, UPPER(name) as upper_name FROM {self.source.name};
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .column("distance", types.Int64)
                .column("time", types.String)
                .column("upper_name", types.String)
                .build()
            ),
        )

        expected_result_df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00", "ALEX"],
                ["Emma", 400, 250, "2022-01-01 08:02:00", "EMMA"],
                ["Alex", 300, 200, "2022-01-02 08:03:00", "ALEX"],
                ["Emma", 200, 250, "2022-01-02 08:04:00", "EMMA"],
                ["Jack", 500, 500, "2022-01-03 08:05:00", "JACK"],
                ["Alex", 600, 800, "2022-01-03 08:06:00", "ALEX"],
            ],
            columns=["name", "cost", "distance", "time", "upper_name"],
        )

        self.client.build_features([features])

        result_df = (
            self.client.get_features(feature_descriptor=features)
            .to_pandas()
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_usage_followed_by_sliding_feature_view(self):
        features: FeatureView = SqlFeatureView(
            name="test_view_name",
            sql_statement=f"""
                SELECT name, cost, `time` FROM {self.source.name};
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .column("time", types.String)
                .build()
            ),
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

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
            source=features,
            features=[f_total_cost],
            extra_props={
                ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: True,
                SKIP_SAME_WINDOW_OUTPUT_CONFIG: True,
            },
        )

        expected_result_df = pd.DataFrame(
            [
                [to_epoch_millis("2022-01-01 23:59:59.999"), 500],
                [to_epoch_millis("2022-01-02 23:59:59.999"), 1000],
                [to_epoch_millis("2022-01-03 23:59:59.999"), 1600],
                [to_epoch_millis("2022-01-04 23:59:59.999"), 1100],
                [to_epoch_millis("2022-01-05 23:59:59.999"), 0],
            ],
            columns=["window_time", "total_cost"],
        )

        self.client.build_features([features])

        result_df = (
            self.client.get_features(feature_descriptor=features)
            .to_pandas()
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_multiple_independent_sql_feature_view(self):
        irrelevant_features = SqlFeatureView(
            name="irrelevant_features",
            sql_statement=f"""
                SELECT name, cost FROM {self.source.name};
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .build()
            ),
        )

        features = SqlFeatureView(
            name="test_view_name",
            sql_statement=f"""
                SELECT name, cost FROM {self.source.name};
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .build()
            ),
        )

        self.client.build_features([irrelevant_features, features])

        expected_result_df = pd.DataFrame(
            [
                ["Alex", 100],
                ["Emma", 400],
                ["Alex", 300],
                ["Emma", 200],
                ["Jack", 500],
                ["Alex", 600],
            ],
            columns=["name", "cost"],
        )

        self.client.build_features([features])

        result_df = (
            self.client.get_features(feature_descriptor=features)
            .to_pandas()
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_usage_with_unbounded_source(self):
        source = DataGenSource(
            name="unbounded_source",
            schema=(
                Schema.new_builder()
                .column("id", types.Int64)
                .column("name", types.String)
                .column("cost", types.Int64)
                .build()
            ),
            keys=["id"],
        )

        self.client.build_features([source])

        features = SqlFeatureView(
            name="test_view_name",
            sql_statement=f"""
                SELECT * FROM {source.name};
            """,
            schema=source.schema,
            is_bounded=False,
        )

        self.client.build_features([features])

        with self.assertRaises(FeathubException) as cm:
            self.client.get_features(features).to_pandas(force_bounded=True)

        self.assertIn(
            "SqlFeatureView is unbounded and it doesn't support getting bounded view.",
            cm.exception.args[0],
        )

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
import tempfile
import unittest
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from feathub.common.exceptions import FeathubException, FeathubTransformationException
from feathub.common.types import String, Int64, Float64
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.joined_feature_view import JoinedFeatureView
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.window_agg_transform import WindowAggTransform
from feathub.processors.flink.flink_table_builder import FlinkTableBuilder
from feathub.registries.local_registry import LocalRegistry
from feathub.sources.file_source import FileSource
from feathub.sources.online_store_source import OnlineStoreSource
from feathub.table.schema import Schema


def _to_timestamp(datetime_str):
    return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")


class FlinkTableBuilderTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

        self.registry = LocalRegistry(config={})

        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)

        self.flink_table_builder = FlinkTableBuilder(
            t_env, registry=self.registry, processor_config={}
        )

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

        self.schema = Schema(
            ["name", "cost", "distance", "time"], [String, Int64, Int64, String]
        )

    def test_get_table_from_file_source(self):
        source = self._create_file_source(self.input_data.copy())
        table = self.flink_table_builder.build(features=source)
        df = table.to_pandas()
        self.assertTrue(self.input_data.equals(df))

    def test_get_table_with_single_key(self):
        source = self._create_file_source(self.input_data.copy(), keys=["name"])
        keys = pd.DataFrame(
            [
                ["Alex"],
                ["Jack"],
                ["Dummy"],
            ],
            columns=["name"],
        )
        result_df = (
            self.flink_table_builder.build(features=source, keys=keys)
            .to_pandas()
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        expected_result_df = (
            pd.DataFrame(
                [
                    ["Alex", 100, 100, "2022-01-01 08:01:00"],
                    ["Alex", 300, 200, "2022-01-02 08:03:00"],
                    ["Jack", 500, 500, "2022-01-03 08:05:00"],
                    ["Alex", 600, 800, "2022-01-03 08:06:00"],
                ],
                columns=["name", "cost", "distance", "time"],
            )
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_with_multiple_keys(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        keys = pd.DataFrame(
            [
                ["Alex", 100],
                ["Alex", 200],
                ["Jack", 500],
                ["Dummy", 300],
            ],
            columns=["name", "cost"],
        )
        result_df = (
            self.flink_table_builder.build(features=source, keys=keys)
            .to_pandas()
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        expected_result_df = (
            pd.DataFrame(
                [
                    ["Alex", 100, 100, "2022-01-01 08:01:00"],
                    ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ],
                columns=["name", "cost", "distance", "time"],
            )
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_with_non_exist_key(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        keys = pd.DataFrame(
            [
                ["Alex", 100],
                ["Alex", 200],
                ["Jack", 500],
                ["Dummy", 300],
            ],
            columns=["name", "invalid_key"],
        )

        with self.assertRaises(FeathubException):
            self.flink_table_builder.build(features=source, keys=keys)

    def test_get_table_with_start_datetime(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        start_datetime = _to_timestamp("2022-01-02 08:03:00")

        result_df = (
            self.flink_table_builder.build(
                features=source, start_datetime=start_datetime
            )
            .to_pandas()
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        expected_result_df = (
            pd.DataFrame(
                [
                    ["Alex", 300, 200, "2022-01-02 08:03:00"],
                    ["Emma", 200, 250, "2022-01-02 08:04:00"],
                    ["Jack", 500, 500, "2022-01-03 08:05:00"],
                    ["Alex", 600, 800, "2022-01-03 08:06:00"],
                ],
                columns=["name", "cost", "distance", "time"],
            )
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_with_end_datetime(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        end_datetime = _to_timestamp("2022-01-02 08:03:00")

        result_df = (
            self.flink_table_builder.build(features=source, end_datetime=end_datetime)
            .to_pandas()
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        expected_result_df = (
            pd.DataFrame(
                [
                    ["Alex", 100, 100, "2022-01-01 08:01:00"],
                    ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ],
                columns=["name", "cost", "distance", "time"],
            )
            .sort_values(by=["name", "cost", "distance", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_missing_timestamp(self):
        df = self.input_data.copy()
        df = df.drop(columns=["time"])
        source = self._create_file_source(df, keys=["name"], timestamp_field=None)
        _datetime = datetime.strptime("2022-01-02 08:03:00", "%Y-%m-%d %H:%M:%S")

        with self.assertRaises(FeathubException):
            self.flink_table_builder.build(
                features=source, end_datetime=_datetime
            ).to_pandas()

        with self.assertRaises(FeathubException):
            self.flink_table_builder.build(
                features=source, start_datetime=_datetime
            ).to_pandas()

    def test_get_table_with_unsupported_feature_view(self):
        with self.assertRaises(FeathubException):
            self.flink_table_builder.build(
                OnlineStoreSource("table", ["a"], "memory", "table")
            )

    def test_keep_source(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            dtype=Float64,
            transform="CAST(cost AS DOUBLE) / CAST(distance AS DOUBLE) + 10",
        )

        f_cost_per_mile2 = Feature(
            name="cost_per_mile_2",
            dtype=Float64,
            transform="CAST(cost AS DOUBLE) / CAST(distance AS DOUBLE) + 5",
        )

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[f_cost_per_mile, f_cost_per_mile2],
            keep_source_fields=True,
        )

        result_df = (
            self.flink_table_builder.build(feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        expected_result_df = df.sort_values(by=["name", "time"]).reset_index(drop=True)
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        )
        expected_result_df["cost_per_mile_2"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 5, axis=1
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_derived_feature_view(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            dtype=Float64,
            transform="CAST(cost AS DOUBLE) / CAST(distance AS DOUBLE) + 10",
        )

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=WindowAggTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_avg_cost = Feature(
            name="avg_cost",
            dtype=Float64,
            transform=WindowAggTransform(
                expr="cost",
                agg_func="AVG",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_max_cost = Feature(
            name="max_cost",
            dtype=Int64,
            transform=WindowAggTransform(
                expr="cost",
                agg_func="MAX",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_min_cost = Feature(
            name="min_cost",
            dtype=Int64,
            transform=WindowAggTransform(
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
            self.flink_table_builder.build(features=features)
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

    def test_derived_feature_with_unsupported_transformation(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f = Feature(
            name="feature_1", dtype=Int64, transform=JoinTransform("table", "feature_1")
        )
        features = DerivedFeatureView(
            name="feature_view", source=source, features=[f], keep_source_fields=True
        )

        with self.assertRaises(FeathubTransformationException):
            self.flink_table_builder.build(features)

    def test_window_agg_transform_with_unsupported_agg_func(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f = Feature(
            name="feature_1",
            dtype=Int64,
            transform=WindowAggTransform(
                "cost", "unsupported_agg", window_size=timedelta(days=2)
            ),
        )
        features = DerivedFeatureView(
            name="feature_view", source=source, features=[f], keep_source_fields=True
        )

        with self.assertRaises(FeathubTransformationException):
            self.flink_table_builder.build(features)

    def test_window_agg_transform_without_key(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=WindowAggTransform(
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

    def test_window_agg_transform_with_limit(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=WindowAggTransform(
                expr="cost", agg_func="SUM", window_size=timedelta(days=2), limit=10
            ),
        )

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
            keep_source_fields=True,
        )

        with self.assertRaises(FeathubTransformationException):
            self.flink_table_builder.build(features).to_pandas()

    def test_window_agg_transform_without_window_size(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=WindowAggTransform(
                expr="cost", agg_func="SUM", group_by_keys=["name"]
            ),
        )

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
            keep_source_fields=True,
        )

        with self.assertRaises(FeathubTransformationException):
            self.flink_table_builder.build(features).to_pandas()

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

        source = self._create_file_source(df)

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_sum",
                    dtype=Int64,
                    transform=WindowAggTransform(
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

    def test_joined_feature_view(self):
        df_1 = self.input_data.copy()
        source = self._create_file_source(df_1)
        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="cost",
                    dtype=Int64,
                    transform="cost",
                ),
                Feature(
                    name="distance",
                    dtype=Int64,
                    transform="distance",
                ),
            ],
            keep_source_fields=True,
        )

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
        source_2 = self._create_file_source(
            df_2,
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
            timestamp_format="%Y-%m-%d,%H:%M:%S",
        )
        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=source_2,
            features=[
                Feature(
                    name="name",
                    dtype=String,
                    transform="name",
                ),
                Feature(
                    name="avg_cost",
                    dtype=Float64,
                    transform="avg_cost",
                    keys=["name"],
                ),
            ],
            keep_source_fields=False,
        )

        feature_view_3 = JoinedFeatureView(
            name="feature_view_3",
            source=feature_view_1,
            features=[
                Feature(
                    name="cost",
                    dtype=Int64,
                    transform="cost",
                ),
                "distance",
                "feature_view_2.avg_cost",
                Feature(
                    name="derived_cost",
                    dtype=Float64,
                    transform="avg_cost * distance",
                ),
            ],
            keep_source_fields=False,
        )

        [feature_view_2, feature_view_3] = self.registry.build_features(
            [feature_view_2, feature_view_3]
        )

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 20000.0, 100000.0, None, 160000.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.flink_table_builder.build(features=feature_view_3)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertIsNone(feature_view_1.keys)
        self.assertListEqual(["name"], feature_view_2.keys)
        self.assertListEqual(["name"], feature_view_3.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_with_multiple_feature_views(self):
        df_1 = self.input_data.copy()
        source = self._create_file_source(df_1)

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    "avg_cost",
                    dtype=Float64,
                    transform=WindowAggTransform(
                        expr="cost",
                        agg_func="AVG",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                    ),
                ),
                Feature("10_times_cost", dtype=Int64, transform="10 * cost"),
            ],
            keep_source_fields=True,
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=["avg_cost", "10_times_cost"],
        )

        self.registry.build_features([feature_view_1, feature_view_2])

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [100.0, 400.0, 200.0, 300.0, 500.0, 450.0]
        )
        expected_result_df["10_times_cost"] = pd.Series(
            [1000, 4000, 3000, 2000, 5000, 6000]
        )
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.flink_table_builder.build(self.registry.get_features("feature_view_2"))
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def _create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
        schema: Optional[Schema] = None,
    ) -> FileSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        df.to_csv(path, index=False, header=False)
        return FileSource(
            "source",
            path,
            "csv",
            schema=schema if schema is not None else self.schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

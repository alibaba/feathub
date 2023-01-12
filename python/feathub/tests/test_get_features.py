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
from abc import ABC
from datetime import datetime

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.common.types import Float64
from feathub.feature_tables.sources.memory_store_source import MemoryStoreSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.tests.feathub_it_test_base import FeathubITTestBase


def _to_timestamp(datetime_str):
    return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")


class GetFeaturesITTest(ABC, FeathubITTestBase):
    def test_get_table_from_file_source(self):
        source = self.create_file_source(self.input_data.copy())
        table = self.client.get_features(features=source)
        df = table.to_pandas()
        self.assertTrue(self.input_data.equals(df))

    def test_get_table_with_single_key(self):
        source = self.create_file_source(self.input_data.copy(), keys=["name"])
        keys = pd.DataFrame(
            [
                ["Alex"],
                ["Jack"],
                ["Dummy"],
            ],
            columns=["name"],
        )
        result_df = (
            self.client.get_features(features=source, keys=keys)
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
        source = self.create_file_source(df, keys=["name"])
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
            self.client.get_features(features=source, keys=keys)
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
        source = self.create_file_source(df, keys=["name"])
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
            self.client.get_features(features=source, keys=keys).to_pandas()

    def test_get_table_with_start_datetime(self):
        df = self.input_data.copy()
        source = self.create_file_source(df, keys=["name"])
        start_datetime = _to_timestamp("2022-01-02 08:03:00")

        result_df = (
            self.client.get_features(features=source, start_datetime=start_datetime)
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
        source = self.create_file_source(df, keys=["name"])
        end_datetime = _to_timestamp("2022-01-02 08:03:00")

        result_df = (
            self.client.get_features(features=source, end_datetime=end_datetime)
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
        source = self.create_file_source(df, keys=["name"], timestamp_field=None)
        _datetime = datetime.strptime("2022-01-02 08:03:00", "%Y-%m-%d %H:%M:%S")

        with self.assertRaises(FeathubException):
            self.client.get_features(
                features=source, end_datetime=_datetime
            ).to_pandas()

        with self.assertRaises(FeathubException):
            self.client.get_features(
                features=source, start_datetime=_datetime
            ).to_pandas()

    def test_get_table_with_unsupported_feature_view(self):
        with self.assertRaises(FeathubException):
            self.client.get_features(
                MemoryStoreSource("table", ["a"], "table")
            ).to_pandas()

    def test_keep_source(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

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
            self.client.get_features(feature_view)
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

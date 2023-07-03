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

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.common.types import Int64, String, Float64, MapType
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class JoinTransformITTest(ABC, FeathubITTestBase):
    def test_join_transform(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)
        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="cost",
                    transform="cost",
                ),
                Feature(
                    name="distance",
                    transform="distance",
                ),
            ],
            keep_source_fields=True,
        )

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01,09:01:00"],
                ["Emma", 400.0, "2022-01-01,09:02:00"],
                ["Alex", 200.0, "2022-01-02,07:03:00"],
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
            source=feature_view_1,
            features=[
                Feature(
                    name="cost",
                    transform="cost",
                ),
                "distance",
                f"{source_2.name}.avg_cost",
            ],
            keep_source_fields=False,
        )

        feature_view_3 = DerivedFeatureView(
            name="feature_view_3",
            source=feature_view_2,
            features=[
                Feature(
                    name="derived_cost",
                    transform="avg_cost * distance",
                ),
            ],
            keep_source_fields=True,
        )

        [_, built_feature_view_2, built_feature_view_3] = self.client.build_features(
            [source_2, feature_view_2, feature_view_3]
        )

        expected_result_df = df_1[["name", "time", "cost", "distance"]]
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 200.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 40000.0, 100000.0, None, 160000.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(feature_descriptor=built_feature_view_3)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertIsNone(feature_view_1.keys)
        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertListEqual(["name"], built_feature_view_3.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_bounded_left_table_join_unbounded_right_table(self):
        source = DataGenSource(
            name="source_1",
            schema=Schema(["id", "val1", "time"], [Int64, Int64, String]),
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
            number_of_rows=1,
        )

        source_2 = DataGenSource(
            name="source_2",
            schema=Schema(["id", "val2", "time"], [Int64, Int64, String]),
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            keys=["id"],
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=["source_2.val2"],
            keep_source_fields=True,
        )

        built_feature_view = self.client.build_features([source_2, feature_view_1])[1]

        with self.assertRaises(FeathubException) as cm:
            self.client.get_features(built_feature_view).to_pandas()

        self.assertIn(
            "Joining a bounded left table with an unbounded right table is currently "
            "not supported.",
            cm.exception.args[0],
        )

    def test_expression_transform_on_joined_field(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01,09:01:00"],
                ["Emma", 400.0, "2022-01-01,09:02:00"],
                ["Alex", 200.0, "2022-01-02,07:03:00"],
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
            ],
            keep_source_fields=False,
        )

        [_, built_feature_view_2] = self.client.build_features(
            [source_2, feature_view_2]
        )

        expected_result_df = df_1[["name", "time", "cost", "distance"]]
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 200.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 40000.0, 100000.0, None, 160000.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(feature_descriptor=built_feature_view_2)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_join_transform_with_zoned_timestamp(self):
        client = self.get_client(
            {
                "common": {
                    "timeZone": "Asia/Shanghai",
                }
            }
        )

        df_1 = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:00:00.000"],
                ["Emma", 400, 250, "2022-01-01 08:00:00.002"],
                ["Alex", 300, 200, "2022-01-01 08:00:00.004"],
                ["Emma", 200, 250, "2022-01-01 08:00:00.006"],
                ["Jack", 500, 500, "2022-01-01 08:00:00.008"],
                ["Alex", 600, 800, "2022-01-01 08:00:00.010"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        source = self.create_file_source(
            df_1,
            schema=Schema(
                ["name", "cost", "distance", "time"], [String, Int64, Int64, String]
            ),
            timestamp_format="%Y-%m-%d %H:%M:%S.%f",
        )
        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="cost",
                    transform="cost",
                ),
                Feature(
                    name="distance",
                    transform="distance",
                ),
            ],
            keep_source_fields=True,
        )

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 08:00:00.001 +0800"],
                ["Emma", 400.0, "2022-01-01 00:00:00.003 +0000"],
                ["Alex", 200.0, "2022-01-01 08:00:00.005 +0800"],
                ["Emma", 300.0, "2022-01-01 00:00:00.007 +0000"],
                ["Jack", 500.0, "2022-01-01 08:00:00.009 +0800"],
                ["Alex", 450.0, "2022-01-01 00:00:00.011 +0000"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self.create_file_source(
            df_2,
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
            timestamp_format="%Y-%m-%d %H:%M:%S.%f %z",
            keys=["name"],
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=[
                Feature(
                    name="cost",
                    transform="cost",
                ),
                "distance",
                f"{source_2.name}.avg_cost",
            ],
            keep_source_fields=False,
        )

        feature_view_3 = DerivedFeatureView(
            name="feature_view_3",
            source=feature_view_2,
            features=[
                Feature(
                    name="derived_cost",
                    transform="avg_cost * distance",
                ),
            ],
            keep_source_fields=True,
        )

        [_, built_feature_view_2, built_feature_view_3] = client.build_features(
            [source_2, feature_view_2, feature_view_3]
        )

        expected_result_df = df_1[["name", "time", "cost", "distance"]]
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
            client.get_features(feature_descriptor=built_feature_view_3)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertIsNone(feature_view_1.keys)
        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertListEqual(["name"], built_feature_view_3.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_join_right_table_without_timestamp(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)
        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01,09:01:00"],
                ["Emma", 400.0, "2022-01-01,09:02:00"],
                ["Alex", 200.0, "2022-01-02,07:03:00"],
                ["Emma", 300.0, "2022-01-02,09:04:00"],
                ["Jack", 500.0, "2022-01-03,09:05:00"],
                ["Alex", 450.0, "2022-01-03,09:06:00"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self.create_file_source(
            df_2,
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
            timestamp_field=None,
            keys=["name"],
        )
        feature_view = DerivedFeatureView(
            name="feature_view_2",
            source=source,
            features=[
                Feature(
                    name="cost",
                    transform="cost",
                ),
                "distance",
                f"{source_2.name}.avg_cost",
            ],
            keep_source_fields=False,
        )

        [_, feature_view] = self.client.build_features([source_2, feature_view])

        with self.assertRaises(FeathubException) as cm:
            self.client.get_features(feature_view).to_pandas()

        message_key_words = {"join", "timestamp", "field"}
        for word in message_key_words:
            self.assertIn(word, cm.exception.args[0].lower())

    def test_join_transform_with_map_lookup(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)

        df_2 = pd.DataFrame(
            [
                ["Alex", {"Alex": 100.0}, "2022-01-01,09:01:00"],
                ["Emma", {"Emma": 400.0}, "2022-01-01,09:02:00"],
                ["Alex", {"Alex": 200.0}, "2022-01-02,07:03:00"],
                ["Emma", {"Emma": 300.0}, "2022-01-02,09:04:00"],
                ["Jack", {"Jack": 500.0}, "2022-01-03,09:05:00"],
                ["Alex", {"Alex": 450.0}, "2022-01-03,09:06:00"],
            ],
            columns=["name", "avg_cost_map", "time"],
        )
        source_2 = self.create_file_source(
            df_2,
            schema=Schema(
                ["name", "avg_cost_map", "time"],
                [String, MapType(String, Float64), String],
            ),
            timestamp_format="%Y-%m-%d,%H:%M:%S",
            keys=["name"],
            data_format="json",
        )
        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                "cost",
                "distance",
                f"{source_2.name}.avg_cost_map['Alex']",
            ],
            keep_source_fields=False,
        )

        [_, built_feature_view] = self.client.build_features([source_2, feature_view])

        expected_result_df = df_1[["name", "time", "cost", "distance"]]
        expected_result_df["_0"] = pd.Series([None, None, 200.0, None, None, 200.0])
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(feature_descriptor=built_feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertIsNone(source.keys)
        self.assertListEqual(["name"], built_feature_view.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_join_field_with_reserved_keyword(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01,09:01:00"],
                ["Emma", 400.0, "2022-01-01,09:02:00"],
                ["Alex", 200.0, "2022-01-02,07:03:00"],
                ["Emma", 300.0, "2022-01-02,09:04:00"],
                ["Jack", 500.0, "2022-01-03,09:05:00"],
                ["Alex", 450.0, "2022-01-03,09:06:00"],
            ],
            columns=["name", "avg_cost", "timestamp"],
        )
        source_2 = self.create_file_source(
            df_2,
            schema=Schema(
                ["name", "avg_cost", "timestamp"],
                [String, Float64, String],
            ),
            timestamp_field="timestamp",
            timestamp_format="%Y-%m-%d,%H:%M:%S",
            keys=["name"],
            data_format="json",
        )
        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                "cost",
                "distance",
                f"{source_2.name}.avg_cost",
            ],
            keep_source_fields=False,
        )

        [_, built_feature_view] = self.client.build_features([source_2, feature_view])

        expected_result_df = df_1[["name", "time", "cost", "distance"]]
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 200.0, 400.0, None, 200.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(feature_descriptor=built_feature_view)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertIsNone(source.keys)
        self.assertListEqual(["name"], built_feature_view.keys)
        self.assertTrue(expected_result_df.equals(result_df))

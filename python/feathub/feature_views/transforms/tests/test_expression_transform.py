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
from dateutil.tz import tz

from feathub.common import types
from feathub.common.types import Float64, String, Int64, Bool, VectorType, MapType
from feathub.common.utils import to_unix_timestamp
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class ExpressionTransformITTest(ABC, FeathubITTestBase):
    def test_expression_transform(self):
        self._test_expression_transform(False)

    def test_expression_transform_keep_source_fields(self):
        self._test_expression_transform(True)

    def _test_expression_transform(self, keep_source_fields: bool):
        source = self.create_file_source(self.input_data.copy())

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            transform="CAST(cost AS DOUBLE) / CAST(distance AS DOUBLE) + 10",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                f_cost_per_mile,
            ],
            keep_source_fields=keep_source_fields,
        )

        result_df = self.client.get_features(features).to_pandas()

        expected_result_df = self.input_data.copy()
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        )

        if keep_source_fields:
            result_df = result_df.sort_values(by=["name", "time"])
            expected_result_df = expected_result_df.sort_values(by=["name", "time"])
        else:
            result_df = result_df.sort_values(by=["time"])
            expected_result_df.drop(["name", "cost", "distance"], axis=1, inplace=True)
            expected_result_df = expected_result_df.sort_values(by=["time"])

        result_df = result_df.reset_index(drop=True)
        expected_result_df = expected_result_df.reset_index(drop=True)

        self.assertIsNone(source.keys)
        self.assertIsNone(features.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_unix_timestamp(self):
        client = self.get_client(
            {
                "common": {
                    "timeZone": "Asia/Shanghai",
                }
            }
        )

        source = self.create_file_source(self.input_data.copy())

        unix_time = Feature(
            name="unix_time",
            transform="UNIX_TIMESTAMP(time)",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                unix_time,
            ],
            keep_source_fields=True,
        )

        result_df = (
            client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()
        expected_result_df["unix_time"] = expected_result_df.apply(
            lambda row: int(
                to_unix_timestamp(
                    row["time"],
                    tz=tz.gettz("Asia/Shanghai"),
                )
            ),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_unix_timestamp_with_timezone(self):
        input_data = pd.DataFrame(
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

        source = self.create_file_source(
            input_data.copy(),
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
        )

        unix_time = Feature(
            name="unix_time",
            transform="UNIX_TIMESTAMP(time, '%Y-%m-%d %H:%M:%S.%f %z')",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                unix_time,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = input_data.copy()
        expected_result_df["unix_time"] = expected_result_df.apply(
            lambda row: int(
                to_unix_timestamp(row["time"], format="%Y-%m-%d %H:%M:%S.%f %z")
            ),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_lower(self):
        source = self.create_file_source(self.input_data.copy())

        lower_name = Feature(
            name="lower_name",
            transform="LOWER(name)",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                lower_name,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()
        expected_result_df["lower_name"] = expected_result_df.apply(
            lambda row: str(row["name"]).lower(),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_concat(self):
        source = self.create_file_source(self.input_data.copy())

        name_distance = Feature(
            name="name_distance",
            transform='CONCAT(name, "_", distance)',
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                name_distance,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()
        expected_result_df["name_distance"] = expected_result_df.apply(
            lambda row: str(row["name"]) + "_" + str(row["distance"]),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_concat_ws(self):
        source = self.create_file_source(self.input_data.copy())

        name_distance = Feature(
            name="name_distance",
            transform='CONCAT_WS("_", name, distance)',
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                name_distance,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()
        expected_result_df["name_distance"] = expected_result_df.apply(
            lambda row: str(row["name"]) + "_" + str(row["distance"]),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_case(self):
        upper_name = Feature(
            name="upper_name",
            transform="""
            CASE
                WHEN name = 'Alex' THEN 'ALEX'
                WHEN name = 'Jack' THEN 'JACK'
                WHEN name = 'Emma' THEN 'EMMA'
            END
            """,
        )

        self._test_case(upper_name)

    def test_case_else(self):
        upper_name = Feature(
            name="upper_name",
            transform="""
            CASE
                WHEN name = 'Alex' THEN 'ALEX'
                WHEN name = 'Jack' THEN 'JACK'
                ELSE 'EMMA'
            END
            """,
        )

        self._test_case(upper_name)

    def _test_case(self, upper_name_feature: Feature):
        source = self.create_file_source(self.input_data.copy())

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                upper_name_feature,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()
        expected_result_df["upper_name"] = expected_result_df.apply(
            lambda row: str(row["name"]).upper(),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_expression_transform_feature_with_dtype(self):
        source = self.create_file_source(self.input_data.copy())

        cost_plus_one = Feature(
            dtype=Float64,
            name="cost_plus_one",
            transform="cost + 1",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                cost_plus_one,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()
        expected_result_df["cost_plus_one"] = expected_result_df.apply(
            lambda row: row["cost"] + 1,
            axis=1,
        ).astype("float64")
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_duplicate_feature_name_with_source(self):
        source = self.create_file_source(self.input_data.copy())

        f_cost = Feature(name="cost", dtype=Int64, transform="cost + 1")

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                f_cost,
            ],
            keep_source_fields=True,
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=["cost"],
            keep_source_fields=True,
        )

        self.client.build_features([feature_view_1, feature_view_2])

        result_df = (
            self.client.get_features(feature_view_2)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()[
            ["name", "distance", "time", "cost"]
        ]
        expected_result_df["cost"] = expected_result_df.apply(
            lambda row: row["cost"] + 1, axis=1
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_map(self):
        input_data = self.input_data.copy().head(2)
        source = self.create_file_source(input_data)

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="row_map",
                    transform="MAP('name', name, "
                    "'cost', JSON_STRING(cost), "
                    "'distance', JSON_STRING(distance), "
                    "'time', time)",
                ),
                Feature(
                    name="cost_distance_map",
                    transform="MAP('cost', cost, 'distance', distance)",
                ),
                Feature(name="map_cost_to_distance", transform="MAP(cost, distance)"),
            ],
            keep_source_fields=True,
        )

        expected_result_df = input_data.copy()
        expected_result_df["row_map"] = [
            {
                "name": "Alex",
                "cost": "100",
                "distance": "100",
                "time": "2022-01-01 08:01:00",
            },
            {
                "name": "Emma",
                "cost": "400",
                "distance": "250",
                "time": "2022-01-01 08:02:00",
            },
        ]
        expected_result_df["cost_distance_map"] = [
            {"cost": 100, "distance": 100},
            {"cost": 400, "distance": 250},
        ]
        expected_result_df["map_cost_to_distance"] = [
            {100: 100},
            {400: 250},
        ]

        result_df = self.client.get_features(feature_view_1).to_pandas()
        self.assertTrue(expected_result_df.equals(result_df))

    def test_json_string(self):
        input_data = pd.DataFrame(
            [[1, True, "Hello", [1, 2, 3], {"a": 1, "b": 2}]],
            columns=["int_v", "bool_v", "string_v", "list_v", "map_v"],
        )

        source = self.create_file_source(
            input_data,
            schema=Schema.new_builder()
            .column("int_v", Int64)
            .column("bool_v", Bool)
            .column("string_v", String)
            .column("list_v", VectorType(Int64))
            .column("map_v", MapType(String, Int64))
            .build(),
            timestamp_field=None,
            data_format="json",
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="null_json_str", transform="JSON_STRING(CAST(NULL AS INTEGER))"
                ),
                Feature(
                    name="int_json_str",
                    transform="JSON_STRING(int_v)",
                ),
                Feature(
                    name="bool_json_str",
                    transform="JSON_STRING(bool_v)",
                ),
                Feature(
                    name="string_json_str",
                    transform="JSON_STRING(string_v)",
                ),
                Feature(
                    name="list_json_str",
                    transform="JSON_STRING(list_v)",
                ),
                Feature(
                    name="map_json_str",
                    transform="JSON_STRING(map_v)",
                ),
            ],
            keep_source_fields=False,
        )

        expected_result_df = pd.DataFrame(
            [[None, "1", "true", '"Hello"', "[1,2,3]", '{"a":1,"b":2}']],
            columns=[
                "null_json_str",
                "int_json_str",
                "bool_json_str",
                "string_json_str",
                "list_json_str",
                "map_json_str",
            ],
        )
        result_df = self.client.get_features(feature_view_1).to_pandas()
        self.assertTrue(expected_result_df.equals(result_df))

    def test_size(self):
        input_data = pd.DataFrame(
            [
                [[1, 2, 3], {"a": 1, "b": 2}],
                [[], {}],
                [None, None],
            ],
            columns=["list_v", "map_v"],
        )

        source = self.create_file_source(
            input_data,
            schema=Schema.new_builder()
            .column("list_v", VectorType(Int64))
            .column("map_v", MapType(String, Int64))
            .build(),
            timestamp_field=None,
            data_format="json",
        )

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(name="list_v_size", transform="SIZE(list_v)"),
                Feature(name="map_v_size", transform="SIZE(map_v)"),
            ],
            keep_source_fields=False,
        )

        expected_result_df = pd.DataFrame(
            [
                [3, 2],
                [0, 0],
                [None, None],
            ],
            columns=[
                "list_v_size",
                "map_v_size",
            ],
        )
        result_df = self.client.get_features(feature_view_1).to_pandas()
        self.assertTrue(expected_result_df.equals(result_df))

    def test_bracket(self):
        input_data = pd.DataFrame(
            [
                ["Alex", "a", 0],
                ["Emma", "b", 1],
                ["Jack", "c", 2],
            ],
            columns=["id", "str_val", "int_val"],
        )

        source = self.create_file_source(
            df=input_data.copy(),
            schema=(
                Schema.new_builder()
                .column("id", types.String)
                .column("str_val", types.String)
                .column("int_val", types.Int64)
                .build()
            ),
            timestamp_field=None,
            data_format="json",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="value_a",
                    transform="MAP(str_val, int_val)['a']",
                ),
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result_df = input_data.copy()
        expected_result_df["value_a"] = expected_result_df.apply(
            lambda row: 0 if row["str_val"] == "a" else None,
            axis=1,
        )

        self.assertTrue(expected_result_df.equals(result_df))

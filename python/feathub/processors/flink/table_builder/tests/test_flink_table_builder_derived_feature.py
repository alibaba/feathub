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

import pandas as pd

from feathub.common.exceptions import FeathubTransformationException
from feathub.common.types import Float64, Int64
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.processors.flink.table_builder.tests.table_builder_test_base import (
    FlinkTableBuilderTestBase,
)


class FlinkTableBuilderDerivedFeatureViewTest(FlinkTableBuilderTestBase):
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
            transform=OverWindowTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_avg_cost = Feature(
            name="avg_cost",
            dtype=Float64,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="AVG",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_max_cost = Feature(
            name="max_cost",
            dtype=Int64,
            transform=OverWindowTransform(
                expr="cost",
                agg_func="MAX",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
            ),
        )
        f_min_cost = Feature(
            name="min_cost",
            dtype=Int64,
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

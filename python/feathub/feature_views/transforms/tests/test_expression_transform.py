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

from feathub.common.types import Float64
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
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
            dtype=Float64,
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

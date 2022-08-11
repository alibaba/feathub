# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd

from feathub.common.test_utils import LocalProcessorTestCase
from feathub.common import types
from feathub.feature_views.feature import Feature
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.joined_feature_view import JoinedFeatureView


class JoinedFeatureViewTest(LocalProcessorTestCase):
    def setUp(self):
        super().setUp()
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

    def tearDown(self):
        super().tearDown()

    # TODO: test joined feature view with per-row transformation

    def test_joined_feature_view(self):
        df_1 = self.input_data.copy()
        source = self._create_file_source(df_1)
        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="cost",
                    dtype=types.Float32,
                    transform="cost",
                ),
                Feature(
                    name="distance",
                    dtype=types.Float32,
                    transform="distance",
                ),
            ],
            keep_source_fields=True,
        )

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Emma", 400.0, "2022-01-01 09:02:00"],
                ["Alex", 200.0, "2022-01-02 09:03:00"],
                ["Emma", 300.0, "2022-01-02 09:04:00"],
                ["Jack", 500.0, "2022-01-03 09:05:00"],
                ["Alex", 450.0, "2022-01-03 09:06:00"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self._create_file_source(df_2)
        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=source_2,
            features=[
                Feature(
                    name="name",
                    dtype=types.Float32,
                    transform="name",
                ),
                Feature(
                    name="avg_cost",
                    dtype=types.Float32,
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
                    dtype=types.Float32,
                    transform="cost",
                ),
                "distance",
                "feature_view_2.avg_cost",
                Feature(
                    name="derived_cost",
                    dtype=types.Float32,
                    transform="avg_cost * distance",
                ),
            ],
            keep_source_fields=False,
        )

        [built_feature_view_2, built_feature_view_3] = self.registry.build_features(
            [feature_view_2, feature_view_3]
        )

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 20000.0, 100000.0, None, 160000.0]
        )
        result_df = self.processor.get_table(features=built_feature_view_3).to_pandas()

        self.assertIsNone(feature_view_1.keys)
        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertListEqual(["name"], built_feature_view_3.keys)
        self.assertTrue(expected_result_df.equals(result_df))

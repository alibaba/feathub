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

from feathub.processors.spark.spark_processor import SparkProcessor

from feathub.common.types import Float64
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.processors.spark.tests.spark_test_utils import SparkProcessorTestBase


class SparkDerivedFeatureViewTest(SparkProcessorTestBase):
    def test_derived_feature_view(self):
        processor = SparkProcessor(
            props={"processor.spark.master": "local[1]"},
            registry=self.registry,
        )

        source = self._create_file_source(self.input_data.copy(), schema=self.schema)

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
            keep_source_fields=False,
        )

        result_df = (
            processor.get_table(features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)
        expected_result_df["cost"] = expected_result_df["cost"].astype("int32")
        expected_result_df["distance"] = expected_result_df["distance"].astype("int32")

        self.assertIsNone(source.keys)
        self.assertIsNone(features.keys)
        self.assertTrue(expected_result_df.equals(result_df))

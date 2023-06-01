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
import tempfile
from abc import ABC

from feathub.common.types import String, Int64
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class MaterializeFeaturesITTest(ABC, FeathubITTestBase):
    def test_job_group(self):
        source = self.create_file_source(self.input_data)

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[Feature(name="double_cost", transform="cost * 2")],
            keep_source_fields=True,
        )
        sink_1 = FileSystemSink(
            path=tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".csv").name,
            data_format="csv",
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=[Feature(name="double_double_cost", transform="double_cost * 2")],
            keep_source_fields=True,
        )
        sink_2 = FileSystemSink(
            path=tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".csv").name,
            data_format="csv",
        )

        job_group = self.client.create_materialization_group()
        job_group.materialize_features(
            feature_descriptor=feature_view_1, sink=sink_1, allow_overwrite=True
        )
        job_group.materialize_features(
            feature_descriptor=feature_view_2, sink=sink_2, allow_overwrite=True
        )
        job_group.execute().wait()

        source_1 = FileSystemSource(
            name="source_1",
            path=sink_1.path,
            data_format=sink_1.data_format,
            schema=Schema.new_builder()
            .column("name", String)
            .column("cost", Int64)
            .column("distance", Int64)
            .column("time", String)
            .column("double_cost", Int64)
            .build(),
        )
        expected_result = self.input_data.copy()
        expected_result["double_cost"] = expected_result["cost"] * 2
        result_df = self.client.get_features(source_1).to_pandas()
        self.assertTrue(expected_result.equals(result_df))

        source_2 = FileSystemSource(
            name="source_2",
            path=sink_2.path,
            data_format=sink_2.data_format,
            schema=Schema.new_builder()
            .column("name", String)
            .column("cost", Int64)
            .column("distance", Int64)
            .column("time", String)
            .column("double_cost", Int64)
            .column("double_double_cost", Int64)
            .build(),
        )
        expected_result["double_double_cost"] = expected_result["double_cost"] * 2
        result_df = self.client.get_features(source_2).to_pandas()
        self.assertTrue(expected_result.equals(result_df))

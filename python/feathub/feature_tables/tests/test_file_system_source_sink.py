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
import unittest
from abc import ABC

from feathub.common.types import Int64
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class FileSystemSourceTest(unittest.TestCase):
    def test_get_bounded_feature_table(self):
        source = FileSystemSource(
            "source",
            "./path",
            "csv",
            Schema.new_builder().column("x", Int64).column("y", Int64).build(),
        )
        self.assertTrue(source.is_bounded())


class FileSystemSourceSinkITTest(ABC, FeathubITTestBase):
    def test_local_file_system_csv_source_sink(self):
        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".csv").name
        self._test_file_system_source_sink(sink_path, "csv")

    def test_local_file_system_json_source_sink(self):
        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".json").name
        self._test_file_system_source_sink(sink_path, "json")

    def _test_file_system_source_sink(self, path: str, data_format: str):
        source = self.create_file_source(self.input_data)

        sink = FileSystemSink(path, data_format)

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        source = FileSystemSource(
            name=self.generate_random_name("source"),
            path=path,
            data_format=data_format,
            schema=source.schema,
        )

        df = self.client.get_features(source).to_pandas()
        self.assertTrue(self.input_data.equals(df))

    # TODO: Add test case to verify allow_overwrite.

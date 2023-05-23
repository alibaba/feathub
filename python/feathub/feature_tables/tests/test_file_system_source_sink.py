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
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd

from feathub.common.types import (
    Int64,
    String,
    Int32,
    Bool,
    VectorType,
)
from feathub.feature_tables.format_config import (
    PROTOBUF_JAR_PATH_CONFIG,
    PROTOBUF_CLASS_NAME_CONFIG,
    IGNORE_PARSE_ERRORS_CONFIG,
)
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.tests.utils import get_protobuf_jar_path
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
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

    def test_local_file_system_protobuf_source_sink(self):
        sink_path = tempfile.NamedTemporaryFile(
            dir=self.temp_dir, suffix=".protobuf"
        ).name
        self._test_file_system_source_sink(
            sink_path,
            "protobuf",
            {
                PROTOBUF_JAR_PATH_CONFIG: get_protobuf_jar_path(),
                PROTOBUF_CLASS_NAME_CONFIG: "org.feathub.proto.FileSystemTestMessage",
            },
        )

    def test_protobuf_all_types(self):
        df = pd.DataFrame(
            [["abc", 1, True, [1, 2]]],
            columns=["string_v", "int64_v", "bool_v", "vector_v"],
        )
        source = self.create_file_source(
            df,
            data_format="json",
            schema=Schema.new_builder()
            .column("string_v", String)
            .column("int64_v", Int64)
            .column("bool_v", Bool)
            .column("vector_v", VectorType(Int32))
            .build(),
            timestamp_field=None,
        )

        features = DerivedFeatureView(
            name="features",
            source=source,
            features=[
                Feature("string_v", transform="string_v"),
                Feature("bytes_v", transform="CAST(string_v AS BYTES)"),
                Feature("int32_v", transform="CAST(int64_v AS INTEGER)"),
                Feature("int64_v", transform="int64_v"),
                Feature("float32_v", transform="CAST(int64_v AS FLOAT)"),
                Feature("float64_v", transform="CAST(int64_v AS DOUBLE)"),
                Feature("bool_v", transform="bool_v"),
                Feature("vector_v", transform="vector_v"),
            ],
            keep_source_fields=False,
        )

        path = tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".protobuf").name
        sink = FileSystemSink(
            path,
            "protobuf",
            {
                PROTOBUF_JAR_PATH_CONFIG: get_protobuf_jar_path(),
                PROTOBUF_CLASS_NAME_CONFIG: "org.feathub.proto.AllTypesTest",
            },
        )

        table = self.client.get_features(features)
        table.execute_insert(sink, allow_overwrite=True)

        source = FileSystemSource(
            name=self.generate_random_name("source"),
            path=path,
            data_format="protobuf",
            schema=table.get_schema(),
            data_format_props={
                PROTOBUF_JAR_PATH_CONFIG: get_protobuf_jar_path(),
                PROTOBUF_CLASS_NAME_CONFIG: "org.feathub.proto.AllTypesTest",
                IGNORE_PARSE_ERRORS_CONFIG: False,
            },
        )

        df = self.client.get_features(source).to_pandas()
        expected_result = pd.DataFrame(
            [["abc", b"abc", 1, 1, 1.0, 1.0, True, [1, 2]]],
            columns=[
                "string_v",
                "bytes_v",
                "int32_v",
                "int64_v",
                "float32_v",
                "float64_v",
                "bool_v",
                "vector_v",
            ],
        ).astype(
            {
                "string_v": str,
                "bytes_v": bytes,
                "int32_v": np.int32,
                "int64_v": np.int64,
                "float32_v": np.float32,
                "float64_v": np.float64,
                "bool_v": bool,
                "vector_v": object,
            }
        )

        self.assertTrue(expected_result.equals(df))

    def _test_file_system_source_sink(
        self,
        path: str,
        data_format: str,
        data_format_props: Optional[Dict[str, Any]] = None,
    ):
        source = self.create_file_source(self.input_data)

        sink = FileSystemSink(path, data_format, data_format_props)

        self.client.materialize_features(
            feature_descriptor=source, sink=sink, allow_overwrite=True
        ).wait()

        source = FileSystemSource(
            name=self.generate_random_name("source"),
            path=path,
            data_format=data_format,
            schema=source.schema,
            data_format_props=data_format_props,
        )

        df = self.client.get_features(source).to_pandas()
        self.assertTrue(self.input_data.equals(df))

    # TODO: Add test case to verify allow_overwrite.

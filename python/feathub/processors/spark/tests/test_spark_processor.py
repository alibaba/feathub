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
import glob
import tempfile
from typing import Optional, Dict

import pandas as pd

from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.tests.test_black_hole_sink import BlackHoleSinkITTest
from feathub.feature_tables.tests.test_datagen_source import DataGenSourceITTest
from feathub.feature_tables.tests.test_file_system_source_sink import (
    FileSystemSourceSinkITTest,
)
from feathub.feature_tables.tests.test_print_sink import PrintSinkITTest
from feathub.feature_views.transforms.tests.test_expression_transform import (
    ExpressionTransformITTest,
)
from feathub.feature_views.transforms.tests.test_over_window_transform import (
    OverWindowTransformITTest,
)
from feathub.feature_views.transforms.tests.test_python_udf_transform import (
    PythonUDFTransformITTest,
)
from feathub.tests.test_online_features import OnlineFeaturesITTest


class SparkProcessorITTest(
    BlackHoleSinkITTest,
    DataGenSourceITTest,
    ExpressionTransformITTest,
    FileSystemSourceSinkITTest,
    PrintSinkITTest,
    OverWindowTransformITTest,
    PythonUDFTransformITTest,
    OnlineFeaturesITTest,
):
    __test__ = True

    @classmethod
    def setUpClass(cls) -> None:
        cls.invoke_all_base_class_setupclass()

    def setUp(self):
        self.invoke_base_class_setup()

    def tearDown(self) -> None:
        self.invoke_base_class_teardown()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.invoke_all_base_class_teardownclass()

    def get_client(self, extra_config: Optional[Dict] = None) -> FeathubClient:
        return self.get_client_with_local_registry(
            {
                "type": "spark",
                "spark": {
                    "master": "local[1]",
                },
            },
            extra_config,
        )

    def test_file_system_source_sink(self):
        source = self.create_file_source(self.input_data)

        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name

        sink = FileSystemSink(sink_path, "csv")

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        files = glob.glob(f"{sink_path}/*.csv")
        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, names=["name", "cost", "distance", "time"])
            df = df.append(csv)
        df = df.sort_values(by=["time"]).reset_index(drop=True)
        self.assertTrue(self.input_data.equals(df))

    def test_over_window_on_join_field(self):
        pass

    def test_over_window_transform_value_counts(self):
        pass

    def test_over_window_transform_row_num(self):
        pass

    def test_over_window_transform_with_different_criteria(self):
        pass

    def test_over_window_transform_with_window_size_and_limit(self):
        pass

    def test_over_window_transform_first_last_value_with_window_size_and_limit(self):
        pass

    def test_over_window_transform_filter_expr_with_window_size_and_limit(self):
        pass

    def test_case(self):
        pass

    def test_case_else(self):
        pass

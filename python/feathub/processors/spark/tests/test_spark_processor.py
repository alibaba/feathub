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

import pandas as pd

from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.processors.spark.spark_processor import SparkProcessor
from feathub.processors.spark.tests.spark_test_utils import SparkProcessorTestBase


class SparkProcessorTest(SparkProcessorTestBase):
    def test_materialize_features(self) -> None:
        processor = SparkProcessor(
            props={"processor.spark.master": "local[1]"},
            registry=self.registry,
        )

        source = self._create_file_source(self.input_data, schema=self.schema)

        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name

        sink = FileSystemSink(sink_path, "csv")

        processor.materialize_features(
            features=source,
            sink=sink,
        ).wait()

        files = glob.glob(f"{sink_path}/*.csv")
        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, names=["name", "cost", "distance", "time"])
            df = df.append(csv)
        df = df.sort_values(by=["time"]).reset_index(drop=True)
        self.assertTrue(self.input_data.equals(df))

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

import os
import tempfile
import unittest
import uuid
from datetime import timedelta
from typing import Optional, List

import pandas as pd
from pyflink import java_gateway
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from feathub.common.types import String, Int64
from feathub.processors.flink.table_builder.flink_table_builder import FlinkTableBuilder
from feathub.registries.local_registry import LocalRegistry
from feathub.feature_tables.sources.file_source import FileSource
from feathub.table.schema import Schema


class FlinkTableBuilderTestBase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

        self.registry = LocalRegistry(config={})

        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)

        self.flink_table_builder = FlinkTableBuilder(t_env, registry=self.registry)

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

        self.schema = Schema(
            ["name", "cost", "distance", "time"], [String, Int64, Int64, String]
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")
        # clean up the java_gateway so that it won't affect other tests.
        with java_gateway._lock:
            if java_gateway._gateway is not None:
                java_gateway._gateway.shutdown()
                java_gateway._gateway = None

    def _create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
        schema: Optional[Schema] = None,
    ) -> FileSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        df.to_csv(path, index=False, header=False)
        return FileSource(
            f"source_{uuid.uuid4()}",
            path,
            "csv",
            schema=schema if schema is not None else self.schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            max_out_of_orderness=timedelta(minutes=1),
        )

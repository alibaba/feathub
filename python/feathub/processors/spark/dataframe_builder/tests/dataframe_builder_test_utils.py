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

import tempfile
import unittest
import uuid
from typing import Optional, List, Union

import pandas as pd
from feathub.common.types import String, Int64
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.spark.dataframe_builder.spark_dataframe_builder import (
    SparkDataFrameBuilder,
)
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.sql import SparkSession


class SparkDataframeBuilderTestBase(unittest.TestCase):
    registry = None
    spark_dataframe_builder = None

    def setUp(self):
        class_name = self.__class__.__name__
        self.spark = SparkSession.builder.appName(class_name).getOrCreate()
        self.registry = LocalRegistry(props={})
        self.spark_dataframe_builder = SparkDataFrameBuilder(
            spark_session=self.spark,
            registry=self.registry
        )
        self.temp_dir = tempfile.mkdtemp()
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

    def tearDown(self):
        self.spark.stop()
        MemoryOnlineStore.get_instance().reset()

    def _create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
        schema: Optional[Schema] = None,
    ) -> FileSystemSource:
        # Add suffix so that it is not inferred with compression, e.g. xz, zip, etc.
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name + ".csv"
        df.to_csv(path, index=False, header=False)
        return FileSystemSource(
            f"source_{str(uuid.uuid4()).replace('-', '')}_table",
            path,
            "csv",
            schema=schema if schema is not None else self.schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    def _compare_dataframes(
            self,
            df1: Union[pd.DataFrame, PandasOnSparkDataFrame],
            df2: Union[pd.DataFrame, PandasOnSparkDataFrame]
    ):
        self.assertEqual(
            df1.shape[0],
            df2.shape[0],
            f"The left dataframe has {df1.shape[0]} rows while "
            f"the right dataframe has {df2.shape[0]} rows."
        )
        self.assertEqual(
            df1.shape[1],
            df2.shape[1],
            f"The left dataframe has {df1.shape[0]} columns while "
            f"the right dataframe has {df2.shape[0]} columns."
        )

        for rowIndex, row in df1.iterrows():
            for columnIndex, value in row.items():
                cell1 = df1.at[rowIndex, columnIndex]
                cell2 = df2.at[rowIndex, columnIndex]
                if not (pd.isnull(cell1) and pd.isnull(cell2)):
                    self.assertEqual(cell1, cell2)

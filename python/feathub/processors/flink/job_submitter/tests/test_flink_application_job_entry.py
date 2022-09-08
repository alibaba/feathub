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
import glob
import os.path
import pickle
import tempfile
import unittest
from datetime import datetime
from typing import Optional, List, Dict, Union

import pandas
import pandas as pd

from feathub.common.types import Int32, String
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.processors.flink.job_submitter import get_application_job_entry_point
from feathub.processors.flink.job_submitter.feathub_job_descriptor import (
    FeathubJobDescriptor,
)
from feathub.processors.flink.job_submitter.flink_application_cluster_job_entry import (
    run_job,
)
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class FlinkApplicationJobEntryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()

    def test_get_application_job_entry(self):
        expected_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "flink_application_cluster_job_entry.py",
            )
        )
        self.assertEqual(expected_path, get_application_job_entry_point())

    def test_application_job_entry(self):
        input_df = pandas.DataFrame([[1], [2], [3]], columns=["id"])
        file_source = self._create_file_source(input_df, Schema(["id"], [Int32]))

        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        sink = FileSystemSink(sink_path, "csv")
        feathub_config_path = self._prepare_feathub_job_config(
            file_source, None, None, None, sink, {}
        )
        run_job(feathub_config_path)

        files = glob.glob(f"{sink_path}/*")
        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, names=["id"])
            df = df.append(csv)
        df = df.sort_values(by=["id"]).reset_index(drop=True)
        self.assertTrue(input_df.equals(df))

    def test_application_job_with_join_feature(self):
        input_df = pandas.DataFrame(
            [
                [1, "2022-01-01 09:01:00"],
                [2, "2022-01-01 09:01:00"],
                [3, "2022-01-01 09:01:00"],
            ],
            columns=["id", "time"],
        )
        file_source = self._create_file_source(
            input_df, Schema(["id", "time"], [Int32, String]), timestamp_field="time"
        )

        dim_df = pandas.DataFrame(
            [
                [1, 4, "2022-01-01 09:00:00"],
                [2, 5, "2022-01-01 09:00:00"],
                [3, 6, "2022-01-01 09:00:00"],
            ],
            columns=["id", "value", "time"],
        )
        dim_source = self._create_file_source(
            dim_df,
            Schema(["id", "value", "time"], [Int32, Int32, String]),
            name="dim",
            timestamp_field="time",
        )
        dim_feature_view = DerivedFeatureView(
            "dim_feature_view",
            source=dim_source,
            features=[Feature("value", dtype=Int32, keys=["id"], transform="value")],
        )
        feature_view = DerivedFeatureView(
            name="feature_view",
            source=file_source,
            features=[
                Feature(
                    name="value",
                    dtype=Int32,
                    transform=JoinTransform("dim_feature_view", "value"),
                    keys=["id"],
                ),
            ],
        )

        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        sink = FileSystemSink(sink_path, "csv")

        job_config = self._prepare_feathub_job_config(
            feature_view,
            None,
            None,
            None,
            sink,
            join_table={"dim_feature_view": dim_feature_view},
        )
        run_job(job_config)

        expected_df = input_df.copy()
        expected_df["value"] = pandas.Series([4, 5, 6])

        files = glob.glob(f"{sink_path}/*")
        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, names=["id", "time", "value"])
            df = df.append(csv)
        df = df.sort_values(by=["id"]).reset_index(drop=True)
        self.assertTrue(expected_df.equals(df))

    def _prepare_feathub_job_config(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
        sink: FeatureTable,
        join_table: Dict[str, TableDescriptor],
    ) -> str:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        job_descriptor = FeathubJobDescriptor(
            features=features,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            sink=sink,
            local_registry_tables=join_table,
            allow_overwrite=True,
            processor_config={},
            registry_type="local",
            registry_config={
                "namespace": "default",
            },
        )

        with open(path, "wb") as f:
            f.write(pickle.dumps(job_descriptor))
        return path

    def _create_file_source(
        self,
        df: pd.DataFrame,
        schema: Schema,
        name: str = "source",
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
    ) -> FileSystemSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        df.to_csv(path, index=False, header=False)
        return FileSystemSource(
            name,
            path,
            "csv",
            schema=schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

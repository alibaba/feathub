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
import glob
import os

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.common.types import to_numpy_dtype
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.table.schema import Schema


def get_dataframe_from_file_source(source: FileSystemSource) -> pd.DataFrame:
    if os.path.isdir(source.path):
        files = glob.glob(f"{source.path}/*.{source.data_format}")
    else:
        files = [source.path]

    df = pd.DataFrame()
    for f in files:
        tmp_df = _get_dataframe_from_file_path(f, source.data_format, source.schema)
        df = df.append(tmp_df)
    return df


def _get_dataframe_from_file_path(
    file_path: str, data_format: str, schema: Schema
) -> pd.DataFrame:
    if data_format == "csv":
        return pd.read_csv(
            file_path,
            names=schema.field_names,
            dtype={
                name: to_numpy_dtype(dtype)
                for name, dtype in zip(schema.field_names, schema.field_types)
            },
        )
    elif data_format == "json":
        return pd.read_json(
            file_path,
            orient="records",
            lines=True,
            dtype={
                name: to_numpy_dtype(dtype)
                for name, dtype in zip(schema.field_names, schema.field_types)
            },
        )

    raise FeathubException(f"Unsupported file format: {data_format}.")


def insert_into_file_sink(df: pd.DataFrame, sink: FileSystemSink) -> None:
    if not os.path.exists(sink.path):
        os.makedirs(sink.path)
    path = os.path.join(sink.path, f"part-0.{sink.data_format}")
    if sink.data_format == "csv":
        df.to_csv(path, header=False)
    elif sink.data_format == "json":
        df.to_json(path, orient="records", lines=True)

    else:
        raise FeathubException(f"Unknown data format: {sink.data_format}.")

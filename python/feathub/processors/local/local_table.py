# Copyright 2022 The FeatHub Authors
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
import typing
from datetime import timedelta
from typing import Optional

import pandas as pd

from feathub.common import types
from feathub.feature_tables.sinks.sink import Sink
from feathub.processors.processor_job import ProcessorJob
from feathub.table.schema import Schema
from feathub.table.table import Table
from feathub.table.table_descriptor import TableDescriptor

if typing.TYPE_CHECKING:
    from feathub.processors.local.local_processor import LocalProcessor


class LocalTable(Table):
    """
    A table that stores data in memory as pandas DataFrame. A LocalTable can only be
    instantiated and processed by LocalProcessor.
    """

    def __init__(
        self,
        processor: "LocalProcessor",
        features: TableDescriptor,
        df: pd.DataFrame,
        timestamp_field: Optional[str],
        timestamp_format: str,
    ):
        """
        :param df: A DataFrame containing rows of this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :timestamp_format: The format of the timestamp field.
        """
        super().__init__(
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.processor = processor
        self.features = features
        self.df = df

    def get_schema(self) -> Schema:
        field_names = []
        field_types = []
        for i in range(len(self.df.columns)):
            field_names.append(self.df.columns[i])
            field_types.append(types.from_numpy_dtype(self.df.dtypes[i]))
        return Schema(field_names=field_names, field_types=field_types)

    def to_pandas(self, force_bounded: bool = False) -> pd.DataFrame:
        return self.df

    def execute_insert(
        self,
        sink: Sink,
        ttl: Optional[timedelta] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        if ttl is not None or not allow_overwrite:
            raise RuntimeError("Unsupported operation.")
        return self.processor.materialize_dataframe(
            features=self.features,
            features_df=self.df,
            sink=sink,
            allow_overwrite=allow_overwrite,
        )

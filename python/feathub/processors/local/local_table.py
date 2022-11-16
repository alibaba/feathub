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

from datetime import timedelta
from typing import Optional

import pandas as pd

from feathub.common import types
from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.processor_job import ProcessorJob
from feathub.table.schema import Schema
from feathub.table.table import Table


class LocalTable(Table):
    """
    A table that stores data in memory as pandas DataFrame. A LocalTable can only be
    instantiated and processed by LocalProcessor.
    """

    def __init__(
        self,
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
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        pass

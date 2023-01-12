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

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional

import pandas as pd

from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.processor_job import ProcessorJob
from feathub.table.schema import Schema


class Table(ABC):
    """
    A table of tabular data where each column corresponds to a feature. Each table
    subclass is bound to a specific processor subclass.
    """

    def __init__(
        self, timestamp_field: Optional[str], timestamp_format: Optional[str]
    ) -> None:
        """
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :timestamp_format: The format of the timestamp field.
        """
        self.timestamp_field = timestamp_field
        self.timestamp_format = timestamp_format

    @abstractmethod
    def get_schema(self) -> Schema:
        """
        Returns a schema showing the names and data types for each field in this table.
        """
        pass

    @abstractmethod
    def to_pandas(self, force_bounded: bool = False) -> pd.DataFrame:
        """
        Returns a Pandas DataFrame containing values of this table.

        :param force_bounded: Whether to force the table to be bounded.
        """
        pass

    @abstractmethod
    def execute_insert(
        self,
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        """
        Starts a job to write features of this table into the given sink according to
        the specified criteria.

        :param sink: Describes the location to write the features.
        :param ttl: Optional. If it is not None, the features data should be purged from
                    the sink after the specified period of time.
        :param allow_overwrite: If it is false, throw error if the features collide with
                                existing data in the given sink.
        :return: A processor job corresponding to this insertion operation.
        """
        pass

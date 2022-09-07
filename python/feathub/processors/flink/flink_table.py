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
import typing
from datetime import timedelta, datetime
from typing import Optional, Union, Any

import pandas as pd
from pyflink.table import (
    Table as NativeFlinkTable,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.flink.flink_deployment_mode import DeploymentMode
from feathub.processors.flink.flink_types_utils import to_feathub_schema
from feathub.processors.processor_job import ProcessorJob
from feathub.table.schema import Schema
from feathub.table.table import Table
from feathub.table.table_descriptor import TableDescriptor

if typing.TYPE_CHECKING:
    from feathub.processors.flink.flink_processor import FlinkProcessor


class FlinkTable(Table):
    """
    The implementation of Feathub Table for Flink.

    It can only be instantiated and processed by FlinkProcessor. It converts the feature
    to native Flink table internally with the FlinkTableBuilder.
    """

    def __init__(
        self,
        flink_processor: "FlinkProcessor",
        feature: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
    ) -> None:
        """
        Instantiate the FlinkTable.

        :param flink_processor: The FlinkProcessor that instantiate this FlinkTable.
        :param feature: Describes the features to be included in the table. If it is a
                        FeatureView, it must be resolved.
        :param keys: Optional. If it is TableDescriptor or DataFrame, it should be
                     transformed into a table of keys. If it is not None, the computed
                     table only include rows whose key fields match at least one
                     row of the keys.
        :param start_datetime: Optional. If it is not None, the `features` table should
                               have a timestamp field. And the table will only
                               include features whose
                               timestamp >= start_datetime. If any field (e.g. minute)
                               is not specified in the start_datetime, we assume this
                               field has the minimum possible value.
        :param end_datetime: Optional. If it is not None, the `features` table should
                             have a timestamp field. And the table will only
                             include features whose timestamp < end_datetime. If any
                             field (e.g. minute) is not specified in the end_datetime,
                             we assume this field has the maximum possible value.
        """
        super().__init__(feature.timestamp_field, feature.timestamp_format)
        self.flink_processor = flink_processor
        self.feature = feature
        self.keys = keys
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

        self._native_flink_table = None

    def get_schema(self) -> Schema:
        schema = self._flink_table.get_schema()
        return to_feathub_schema(schema)

    def to_pandas(self) -> pd.DataFrame:
        if self.flink_processor.deployment_mode != DeploymentMode.SESSION:
            raise FeathubException("Table.to_pandas is only supported in session mode.")
        return self._flink_table.to_pandas()

    def execute_insert(
        self,
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        return self.flink_processor.materialize_features(
            features=self.feature,
            sink=sink,
            ttl=ttl,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            allow_overwrite=allow_overwrite,
        )

    @property
    def _flink_table(self) -> NativeFlinkTable:
        if self._native_flink_table is None:
            self._native_flink_table = self.flink_processor.flink_table_builder.build(
                features=self.feature,
                keys=self.keys,
                start_datetime=self.start_datetime,
                end_datetime=self.end_datetime,
            )

        return self._native_flink_table

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FlinkTable)
            and self.feature == other.feature
            and self.keys == other.keys
            and self.start_datetime == other.start_datetime
            and self.end_datetime == other.end_datetime
            and self.flink_processor == other.flink_processor
        )

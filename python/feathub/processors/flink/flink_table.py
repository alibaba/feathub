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
from collections import defaultdict
from datetime import timedelta, datetime
from typing import Optional, Union, Any, Dict, List

import numpy as np
import pandas as pd
from pyflink.table import (
    Table as NativeFlinkTable,
    DataTypes,
)

from feathub.common.exceptions import FeathubException
from feathub.common.types import MapType
from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.flink.flink_deployment_mode import DeploymentMode
from feathub.processors.flink.flink_types_utils import to_feathub_schema
from feathub.processors.processor_job import ProcessorJob
from feathub.table.schema import Schema
from feathub.table.table import Table
from feathub.table.table_descriptor import TableDescriptor

if typing.TYPE_CHECKING:
    from feathub.processors.flink.flink_processor import FlinkProcessor

# A type mapping from Flink DataType to numpy type that cannot be derived unambiguously
# by pandas. E.g. Flink DataType of TINYINT is represented in python as type int, which
# can map to int16, int32 and int64 in numpy.
FLINK_DATA_TYPE_TO_NUMPY_TYPE: Dict = {
    type(DataTypes.TINYINT()): np.int8,
    type(DataTypes.SMALLINT()): np.int16,
    type(DataTypes.INT()): np.int32,
    type(DataTypes.BIGINT()): np.int64,
    type(DataTypes.FLOAT()): np.float32,
    type(DataTypes.DOUBLE()): np.float64,
}


# PyFlink Table#to_pandas currently doesn't support Map type. We have to collect the
# result and construct the pandas DataFrame.
# TODO: Use PyFlink Table#to_pandas after
#  https://issues.apache.org/jira/projects/FLINK/issues/FLINK-30607 is resolved.
def flink_table_to_pandas(table: NativeFlinkTable) -> pd.DataFrame:
    """
    Converting the given flink table to pandas dataframe.
    """
    schema = table.get_schema()
    field_names = schema.get_field_names()
    field_types = {
        name: FLINK_DATA_TYPE_TO_NUMPY_TYPE.get(
            type(schema.get_field_data_type(name)), None
        )
        for name in field_names
    }
    with table.execute().collect() as results:
        data: Dict[str, List[Any]] = defaultdict(list)
        for row in results:
            for name, value in zip(field_names, row):
                data[name].append(value)

        return pd.DataFrame(
            {
                name: pd.Series(values, dtype=field_types[name])
                for name, values in data.items()
            }
        )


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

    def get_schema(self) -> Schema:
        schema = self._get_flink_table(self.feature).get_schema()
        return to_feathub_schema(schema)

    def to_pandas(self, force_bounded: bool = False) -> pd.DataFrame:
        if self.flink_processor.deployment_mode not in (
            DeploymentMode.CLI,
            DeploymentMode.SESSION,
        ):
            raise FeathubException(
                "Table.to_pandas is only supported in cli mode and session mode."
            )

        feature = self.feature
        if not feature.is_bounded():
            if not force_bounded:
                raise FeathubException(
                    "Unbounded table cannot be converted to Pandas DataFrame. You can "
                    "set force_bounded to True to convert the Table to DataFrame."
                )
            feature = feature.get_bounded_view()

        return flink_table_to_pandas(self._get_flink_table(feature))

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

    def _get_flink_table(self, feature: TableDescriptor) -> NativeFlinkTable:
        return self.flink_processor.flink_table_builder.build(
            features=feature,
            keys=self.keys,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FlinkTable)
            and self.feature == other.feature
            and self.keys == other.keys
            and self.start_datetime == other.start_datetime
            and self.end_datetime == other.end_datetime
            and self.flink_processor == other.flink_processor
        )

    def _has_map_type(self) -> bool:
        for dtype in self.get_schema().field_types:
            if isinstance(dtype, MapType):
                return True
        return False

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
from datetime import timedelta, datetime
from typing import Optional, Union

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.sink import Sink
from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.processors.processor_job import ProcessorJob
from feathub.processors.spark.spark_types_utils import (
    to_feathub_schema,
)
from feathub.table.schema import Schema
from feathub.table.table import Table
from feathub.table.table_descriptor import TableDescriptor

if typing.TYPE_CHECKING:
    from feathub.processors.spark.spark_processor import SparkProcessor


class SparkTable(Table):
    """
    The implementation of FeatHub Table for Spark.

    It can only be instantiated and processed by SparkProcessor. It converts the feature
    to native Spark DataFrame internally with the SparkDataFrameBuilder.
    """

    def __init__(
        self,
        feature: TableDescriptor,
        spark_processor: "SparkProcessor",
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
    ) -> None:
        """
        Instantiate the SparkTable.

        :param feature: Describes the features to be included in the table. If it is a
                        FeatureView, it must be resolved.
        :param spark_processor: The spark processor to materialize features.
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
        self._feature = feature
        self._spark_processor = spark_processor
        self._keys = keys
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    def get_schema(self) -> Schema:
        return to_feathub_schema(
            self._spark_processor.get_spark_dataframe(
                feature=self._feature,
                keys=self._keys,
                start_datetime=self.start_datetime,
                end_datetime=self.end_datetime,
            ).schema
        )

    def to_pandas(self, force_bounded: bool = False) -> pd.DataFrame:
        feature = self._feature
        if not feature.is_bounded():
            if not force_bounded:
                raise FeathubException(
                    "Unbounded table cannot be converted to Pandas DataFrame. You can "
                    "set force_bounded to True to convert the Table to DataFrame."
                )
            feature = feature.get_bounded_view()

        dataframe = self._spark_processor.get_spark_dataframe(
            feature=feature,
            keys=self._keys,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
        )

        return dataframe.toPandas()

    def execute_insert(
        self,
        sink: Sink,
        ttl: Optional[timedelta] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        return self._spark_processor.materialize_features(
            materialization_descriptors=[
                MaterializationDescriptor(
                    feature_descriptor=self._feature,
                    sink=sink,
                    ttl=ttl,
                    allow_overwrite=allow_overwrite,
                    start_datetime=self.start_datetime,
                    end_datetime=self.end_datetime,
                )
            ]
        )

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, SparkTable)
            and self._feature == other._feature
            and self._spark_processor == other._spark_processor
            and self._keys == other._keys
            and self.start_datetime == other.start_datetime
            and self.end_datetime == other.end_datetime
        )

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
import typing
from datetime import timedelta
from typing import Optional

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.spark.spark_job import SparkJob
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
    The implementation of Feathub Table for Spark.

    It can only be instantiated and processed by SparkProcessor. It converts the feature
    to native Spark DataFrame internally with the SparkDataFrameBuilder.
    """

    def __init__(
        self,
        feature: TableDescriptor,
        spark_processor: "SparkProcessor",
    ) -> None:
        """
        Instantiate the SparkTable.

        :param feature: Describes the features to be included in the table. If it is a
                        FeatureView, it must be resolved.
        :param spark_processor: The spark processor to materialize features.
        """
        super().__init__(feature.timestamp_field, feature.timestamp_format)
        self._feature = feature
        self._spark_processor = spark_processor

    def get_schema(self) -> Schema:
        return to_feathub_schema(
            self._spark_processor.get_spark_dataframe(self._feature).schema
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

        dataframe = self._spark_processor.get_spark_dataframe(feature)

        return dataframe.toPandas()

    def execute_insert(
        self,
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        allow_overwrite: bool = False,
    ) -> SparkJob:
        return self._spark_processor.materialize_features(
            features=self._feature,
            sink=sink,
            ttl=ttl,
            allow_overwrite=allow_overwrite,
        )

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, SparkTable)
            and self._feature == other._feature
            and self._spark_processor == other._spark_processor
        )

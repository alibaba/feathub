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
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime
from typing import Union, Optional, Dict

import pandas as pd
from pyspark.sql import DataFrame as NativeSparkDataFrame
from pyspark.sql import SparkSession

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.feature_view import FeatureView
from feathub.processors.processor import Processor
from feathub.processors.spark.dataframe_builder.source_sink_utils import (
    insert_into_sink,
)
from feathub.processors.spark.dataframe_builder.spark_dataframe_builder import (
    SparkDataFrameBuilder,
)
from feathub.processors.spark.spark_job import SparkJob
from feathub.processors.spark.spark_processor_config import (
    SparkProcessorConfig,
    MASTER_CONFIG,
)
from feathub.processors.spark.spark_table import SparkTable
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class SparkProcessor(Processor):
    """
    The SparkProcessor does feature ETL using Spark as the processing engine.

    In the following we describe the keys accepted by the `config` dict passed to the
    SparkProcessor constructor.

    master: The Spark master URL to connect to.
    """

    def __init__(self, props: Dict, registry: Registry):
        """
        Instantiate the SparkProcessor.

        :param props: The processor properties.
        :param registry: An entity registry.
        """
        super().__init__()
        self._registry = registry

        config = SparkProcessorConfig(props)

        spark_session = SparkSession.builder.master(
            config.get(MASTER_CONFIG)
        ).getOrCreate()

        self._dataframe_builder = SparkDataFrameBuilder(spark_session, self._registry)

        self._executor = ThreadPoolExecutor()

    def get_table(
        self,
        features: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> SparkTable:
        if keys is not None:
            # TODO: Add support for keys after Spark processor supports join transform.
            raise FeathubException(
                "Spark processor does not support features with keys."
            )

        if start_datetime is not None or end_datetime is not None:
            # TODO: Add support for timestamp and watermark with window transform.
            raise FeathubException(
                "Spark processor does not support filtering features with "
                "start/end datetime."
            )

        features = self._resolve_table_descriptor(features)

        return SparkTable(
            feature=features,
            spark_processor=self,
        )

    def materialize_features(
        self,
        features: Union[str, TableDescriptor],
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ) -> SparkJob:
        if ttl is not None:
            # TODO: Add support for sinks with ttl.
            raise FeathubException(
                "Spark processor does not support inserting features with ttl."
            )

        resolved_features = self._resolve_table_descriptor(features)

        dataframe = self.get_spark_dataframe(resolved_features)

        future = insert_into_sink(
            executor=self._executor,
            dataframe=dataframe,
            sink=sink,
            allow_overwrite=allow_overwrite,
        )

        return SparkJob(future)

    def _resolve_table_descriptor(
        self, features: Union[str, TableDescriptor]
    ) -> TableDescriptor:
        if isinstance(features, str):
            features = self._registry.get_features(name=features)
        elif isinstance(features, FeatureView) and features.is_unresolved():
            features = self._registry.get_features(name=features.name)

        return features

    def get_spark_dataframe(self, feature: TableDescriptor) -> NativeSparkDataFrame:
        return self._dataframe_builder.build(feature)

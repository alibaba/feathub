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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Union, Optional, Dict, Sequence

import pandas as pd
from pyspark.sql import DataFrame as NativeSparkDataFrame
from pyspark.sql import SparkSession

from feathub.common.config import TIMEZONE_CONFIG
from feathub.common.exceptions import FeathubException, FeathubConfigurationException
from feathub.feature_views.feature_view import FeatureView
from feathub.processors.processor import (
    Processor,
)
from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.processors.processor_job import ProcessorJob
from feathub.processors.spark.dataframe_builder.source_sink_utils import (
    insert_into_sink,
)
from feathub.processors.spark.dataframe_builder.spark_dataframe_builder import (
    SparkDataFrameBuilder,
)
from feathub.processors.spark.spark_job import SparkJob, CombinedSparkJob
from feathub.processors.spark.spark_processor_config import (
    SparkProcessorConfig,
    MASTER_CONFIG,
    NATIVE_CONFIG_PREFIX,
    NATIVE_CONFIG_PROCESSOR_CONFIG_MAP,
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
    native.*: Any key with the "native" prefix will be forwarded to the Spark Session
              config after the "native" prefix is removed. For example, if the processor
              config has an entry "native.spark.default.parallelism": 2, then the Spark
              Session config will have an entry "spark.default.parallelism": 2.
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

        spark_session_builder = SparkSession.builder
        spark_session_builder = spark_session_builder.master(config.get(MASTER_CONFIG))
        spark_session_builder = spark_session_builder.config(
            "spark.sql.session.timeZone", config.get(TIMEZONE_CONFIG)
        ).config("spark.sql.legacy.sizeOfNull", False)

        prefix_len = len(NATIVE_CONFIG_PREFIX)
        for k, v in config.original_props_with_prefix(
            NATIVE_CONFIG_PREFIX, False
        ).items():
            if (
                k in NATIVE_CONFIG_PROCESSOR_CONFIG_MAP
                and NATIVE_CONFIG_PROCESSOR_CONFIG_MAP[k] in config.config_values
                and v != config.config_values[NATIVE_CONFIG_PROCESSOR_CONFIG_MAP[k]]
            ):
                raise FeathubConfigurationException(
                    f"Native config: {k} is conflict with processor config: "
                    f"{NATIVE_CONFIG_PROCESSOR_CONFIG_MAP[k]}."
                )
            spark_session_builder = spark_session_builder.config(k[prefix_len:], v)
        spark_session = spark_session_builder.getOrCreate()

        self._dataframe_builder = SparkDataFrameBuilder(spark_session, self._registry)

        self._executor = ThreadPoolExecutor()

    def get_table(
        self,
        feature_descriptor: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> SparkTable:
        feature_descriptor = self._resolve_table_descriptor(feature_descriptor)

        return SparkTable(
            feature=feature_descriptor,
            spark_processor=self,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def materialize_features(
        self,
        materialization_descriptors: Sequence[MaterializationDescriptor],
    ) -> ProcessorJob:
        # TODO: Optimize performance by reusing intermediate results.
        spark_jobs = []
        for materialization_descriptor in materialization_descriptors:
            if materialization_descriptor.ttl is not None:
                raise FeathubException(
                    "Spark processor does not support inserting features with ttl."
                )

            resolved_features = self._resolve_table_descriptor(
                materialization_descriptor.feature_descriptor
            )

            dataframe = self.get_spark_dataframe(
                feature=resolved_features,
                start_datetime=materialization_descriptor.start_datetime,
                end_datetime=materialization_descriptor.end_datetime,
            )

            future = insert_into_sink(
                executor=self._executor,
                dataframe=dataframe,
                features_desc=resolved_features,
                sink=materialization_descriptor.sink,
                allow_overwrite=materialization_descriptor.allow_overwrite,
            )

            spark_jobs.append(SparkJob(job_future=future))

        if len(spark_jobs) == 1:
            return spark_jobs[0]
        else:
            return CombinedSparkJob(spark_jobs, self._executor)

    def _resolve_table_descriptor(
        self, features: Union[str, TableDescriptor]
    ) -> TableDescriptor:
        if isinstance(features, str):
            features = self._registry.get_features(name=features)
        elif isinstance(features, FeatureView) and features.is_unresolved():
            features = self._registry.build_features([features])[0]

        return features

    def get_spark_dataframe(
        self,
        feature: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> NativeSparkDataFrame:
        return self._dataframe_builder.build(
            feature, keys, start_datetime, end_datetime
        )

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

from __future__ import annotations
from typing import Dict, Union, Optional
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import pandas as pd

from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.processor_config import (
    ProcessorConfig,
    ProcessorType,
    PROCESSOR_TYPE_CONFIG,
)
from feathub.table.table import Table
from feathub.table.table_descriptor import TableDescriptor
from feathub.processors.processor_job import ProcessorJob
from feathub.registries.registry import Registry


class Processor(ABC):
    """
    A processor is a pluggable processing engine that provides APIs to extract,
    transform and load feature data into feature stores. It should recognize all table
    descriptors and translate those declarative descriptors into processing jobs.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def get_table(
        self,
        features: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> Table:
        """
        Returns a table of features according to the specified criteria.

        :param features: Describes the features to be included in the table. If it is a
                         string, it refers to the name of a table descriptor in the
                         entity registry.
        :param keys: Optional. If it is TableDescriptor or DataFrame, it should be
                     transformed into a table of keys. If it is not None, the output
                     table should only include rows whose key fields match at least one
                     row of the keys.
        :param start_datetime: Optional. If it is not None, the `features` table should
                               have a timestamp field. And the output table will only
                               include features whose
                               timestamp >= start_datetime. If any field (e.g. minute)
                               is not specified in the start_datetime, we assume this
                               field has the minimum possible value.
        :param end_datetime: Optional. If it is not None, the `features` table should
                             have a timestamp field. And the output table will only
                             include features whose timestamp < end_datetime. If any
                             field (e.g. minute) is not specified in the end_datetime,
                             we assume this field has the maximum possible value.
        :return: A table of features.
        """
        pass

    @abstractmethod
    def materialize_features(
        self,
        features: Union[str, TableDescriptor],
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        """
        Starts a job to write a table of features into the given sink according to the
        specified criteria.

        :param features: Describes the table of features to be inserted in the sink. If
                         it is a string, it refers to the name of a table descriptor in
                         the entity registry.
        :param sink: Describes the location to write the features.
        :param ttl: Optional. If it is not None, the features data should be purged from
                    the sink after the specified period of time.
        :param start_datetime: Optional. If it is not None, the `features` table should
                               have a timestamp field. And only writes into sink those
                               features whose timestamp >= floor(start_datetime).
        :param end_datetime: Optional. If it is not None, the `features` table should
                             have a timestamp field. And only writes into sink those
                             features whose timestamp <= ceil(start_datetime).
        :param allow_overwrite: If it is false, throw error if the features collide with
                                existing data in the given sink.
        :return: A processor job corresponding to this materialization operation.
        """
        pass

    @staticmethod
    def instantiate(
        props: Dict,
        registry: Registry,
    ) -> Processor:
        """
        Instantiates a processor using the given properties and the store instances.
        """

        processor_config = ProcessorConfig(props)
        processor_type = ProcessorType(processor_config.get(PROCESSOR_TYPE_CONFIG))

        if processor_type == ProcessorType.LOCAL:
            from feathub.processors.local.local_processor import LocalProcessor

            return LocalProcessor(props=props, registry=registry)
        elif processor_type == ProcessorType.FLINK:
            from feathub.processors.flink.flink_processor import FlinkProcessor

            return FlinkProcessor(props=props, registry=registry)
        elif processor_type == ProcessorType.SPARK:
            from feathub.processors.spark.spark_processor import SparkProcessor

            return SparkProcessor(props=props, registry=registry)

        raise RuntimeError(f"Failed to instantiate processor with props={props}.")

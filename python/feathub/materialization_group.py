#  Copyright 2022 The FeatHub Authors
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

from datetime import timedelta, datetime
from typing import List, Union, Optional

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.sink import Sink
from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.processors.processor import Processor
from feathub.processors.processor_job import ProcessorJob
from feathub.table.table_descriptor import TableDescriptor


class MaterializationGroup:
    """
    Group multiple feature materialization and execute them as one processor job when
    `execute` is invoked.
    """

    def __init__(self, processor: Processor):
        self._processor = processor
        self._materialization_operation_descriptors: List[
            MaterializationDescriptor
        ] = []

    def materialize_features(
        self,
        feature_descriptor: Union[str, TableDescriptor],
        sink: Sink,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ) -> None:
        """
        Add a materialization that write the feature table to the sink with
        the specified criteria to the materialization group.

        :param feature_descriptor: Describes the table of features to be inserted in the
                                   sink. If it is a string, it refers to the name of a
                                   table descriptor in the entity registry.
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
        """
        self._materialization_operation_descriptors.append(
            MaterializationDescriptor(
                feature_descriptor=feature_descriptor,
                sink=sink,
                ttl=ttl,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                allow_overwrite=allow_overwrite,
            )
        )

    def execute(self) -> ProcessorJob:
        """
        Execute the materialization operations. The materialization operations will be
        cleared after calling this method.

        :return: A processor job corresponding to the materialization operations.
        """
        if not self._materialization_operation_descriptors:
            raise FeathubException(
                "There should be at least one materialization descriptor."
            )
        processor_job = self._processor.materialize_features(
            self._materialization_operation_descriptors
        )
        self._materialization_operation_descriptors.clear()
        return processor_job

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
from abc import ABC, abstractmethod
from typing import Dict, Sequence

from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.processors.processor_job import ProcessorJob
from feathub.table.table_descriptor import TableDescriptor


class FlinkJobSubmitter(ABC):
    """FlinkJobSubmitter is an interface to submit a Flink job."""

    @abstractmethod
    def submit(
        self,
        materialization_descriptors: Sequence[MaterializationDescriptor],
        local_registry_tables: Dict[str, TableDescriptor],
    ) -> ProcessorJob:
        """
        Submit a Flink job that materialize feature views into sinks.

        :param materialization_descriptors: A list of materialization descriptors.
        :param local_registry_tables: All the table descriptors registered in the local
                                      registry that are required to compute the given
                                      table.
        :return: A processor job representing the submitted Flink job.
        """
        pass

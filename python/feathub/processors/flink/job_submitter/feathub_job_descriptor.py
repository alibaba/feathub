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
from typing import Dict, Any, Sequence

from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.table.table_descriptor import TableDescriptor


class FeathubJobDescriptor:
    """Descriptor of a FeatHub job to run in a remote Flink cluster."""

    def __init__(
        self,
        materialization_descriptors: Sequence[MaterializationDescriptor],
        local_registry_tables: Dict[str, TableDescriptor],
        props: Dict,
    ):
        """
        Instantiate a FeathubJobDescriptor.

        :param materialization_descriptors: A list of materialization descriptors.
        :param local_registry_tables: All the table descriptors registered in the local
                                      registry that are required to compute the given
                                      table.
        :param props: All properties of FeatHub.
        """
        self.materialization_descriptors = materialization_descriptors
        self.local_registry_tables = local_registry_tables
        self.props = props

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FeathubJobDescriptor)
            and self.materialization_descriptors == other.materialization_descriptors
            and self.local_registry_tables == other.local_registry_tables
            and self.props == other.props
        )

    def __hash__(self) -> int:
        return hash(
            (self.materialization_descriptors, self.local_registry_tables, self.props)
        )

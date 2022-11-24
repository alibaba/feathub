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
from datetime import datetime
from typing import Optional, Dict, Any, Union

import pandas as pd

from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.table_descriptor import TableDescriptor


class FeathubJobDescriptor:
    """Descriptor of a Feathub job to run in a remote Flink cluster."""

    def __init__(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
        sink: FeatureTable,
        local_registry_tables: Dict[str, TableDescriptor],
        allow_overwrite: bool,
        config: Dict,
    ):
        """
        Instantiate a FeathubJobDescriptor.

        :param features: The table descriptor that contains the features to compute.
        :param keys: Optional. If it is TableDescriptor or DataFrame, it should be
                     transformed into a table of keys. If it is not None, the
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
        :param sink: Where the features write to.
        :param local_registry_tables: All the table descriptors registered in the local
                                      registry that are required to compute the given
                                      table.
        :param allow_overwrite: If it is true, throw error if the features collide with
                                existing data in the given sink.
        :param config: All configurations of Feathub.
        """
        self.features = features
        self.keys = keys
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.sink = sink
        self.local_registry_tables = local_registry_tables
        self.allow_overwrite = allow_overwrite
        self.config = config

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FeathubJobDescriptor)
            and self.features == other.features
            and self.keys == other.keys
            and self.start_datetime == other.start_datetime
            and self.end_datetime == other.end_datetime
            and self.sink == other.sink
            and self.local_registry_tables == other.local_registry_tables
            and self.allow_overwrite == other.allow_overwrite
            and self.config == other.config
        )

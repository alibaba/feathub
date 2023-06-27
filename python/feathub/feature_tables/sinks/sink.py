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
from abc import ABC
from typing import Any, Dict, Optional

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.table_descriptor import TableDescriptor


class Sink(FeatureTable, ABC):
    """
    Base class for all Sink Feature Table.
    """

    def __init__(
        self,
        name: str,
        system_name: str,
        table_uri: Dict[str, Any],
        data_format: Optional[str] = None,
        data_format_props: Optional[Dict[str, Any]] = None,
        keep_timestamp_field: bool = True,
    ):
        """
        :param name: The name that uniquely identifies this feature table in a registry.
        :param system_name: Uniquely identifies the underlying system, e.g. filesystem,
                            kafka, etc.
        :param table_uri: It contains the properties specific to the underlying system
                           that are used to uniquely identify the physical table.
        :param data_format: Optional. If it is not None, it specifies the format of the
                            data, e.g. csv, json, parquet, etc. This is typically
                            used by storage that does not require schema, e.g.
                            filesystem, kafka, etc.
        :param data_format_props: The properties of the data format.
        :param keep_timestamp_field: True if the timestamp field of the feature table
                                     should be persisted to the external system through
                                     the sink.
        """
        super().__init__(
            name=name,
            system_name=system_name,
            table_uri=table_uri,
            data_format=data_format,
            data_format_props=data_format_props,
        )
        self.keep_timestamp_field = keep_timestamp_field

    def is_bounded(self) -> bool:
        raise FeathubException(
            "Sink feature table doesn't have boundedness. "
            "This method should not be called."
        )

    def get_bounded_view(self) -> TableDescriptor:
        raise FeathubException(f"Cannot get bounded feature table on {type(self)}.")

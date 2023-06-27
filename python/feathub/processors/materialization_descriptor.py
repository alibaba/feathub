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

from __future__ import annotations

from datetime import timedelta, datetime
from typing import Union, Optional, Any

from feathub.feature_tables.sinks.sink import Sink
from feathub.table.table_descriptor import TableDescriptor


class MaterializationDescriptor:
    """
    Descriptor of a materialization.
    """

    def __init__(
        self,
        feature_descriptor: Union[str, TableDescriptor],
        sink: Sink,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ):
        """
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
        self.feature_descriptor = feature_descriptor
        self.sink = sink
        self.ttl = ttl
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.allow_overwrite = allow_overwrite

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, MaterializationDescriptor)
            and self.feature_descriptor == other.feature_descriptor
            and self.sink == other.sink
            and self.ttl == other.ttl
            and self.start_datetime == other.start_datetime
            and self.end_datetime == other.end_datetime
            and self.allow_overwrite == other.allow_overwrite
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.feature_descriptor,
                self.sink,
                self.ttl,
                self.start_datetime,
                self.end_datetime,
                self.allow_overwrite,
            )
        )

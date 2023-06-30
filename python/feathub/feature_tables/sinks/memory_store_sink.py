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
from typing import Dict

from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.sinks.sink import Sink


class MemoryStoreSink(Sink):
    """
    A sink corresponding to a table in an online feature store.
    """

    def __init__(
        self,
        table_name: str,
        keep_timestamp_field: bool = True,
    ):
        """
        :param table_name: The name of a table in the feature store.
        :param keep_timestamp_field: True if the timestamp field of the feature table
                                     should be persisted to the external system through
                                     the sink.
        """
        super().__init__(
            name="",
            system_name="memory",
            table_uri={"table_name": table_name},
            keep_timestamp_field=keep_timestamp_field,
        )
        self.table_name = table_name

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "table_name": self.table_name,
            "keep_timestamp_field": self.keep_timestamp_field,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "MemoryStoreSink":
        return MemoryStoreSink(
            table_name=json_dict["table_name"],
            keep_timestamp_field=json_dict["keep_timestamp_field"],
        )

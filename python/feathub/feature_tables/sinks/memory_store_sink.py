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
from typing import Dict

from feathub.feature_tables.sinks.sink import Sink


class MemoryStoreSink(Sink):
    """
    A sink corresponding to a table in an online feature store.
    """

    def __init__(self, table_name: str):
        """
        :param table_name: The name of a table in the feature store.
        """
        super().__init__(
            name="",
            system_name="memory",
            properties={"table_name": table_name},
        )
        self.table_name = table_name

    def to_json(self) -> Dict:
        return {
            "type": "MemoryStoreSink",
            "table_name": self.table_name,
        }
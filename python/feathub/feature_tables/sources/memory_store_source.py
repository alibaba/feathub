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

from typing import List, Dict, Optional

from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.feature_table import FeatureTable
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class MemoryStoreSource(FeatureTable):
    """
    A source corresponding to a table in an online feature store.
    """

    def __init__(
        self,
        name: str,
        keys: List[str],
        table_name: str,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param keys: The keys of the table in the online feature store.
        :param table_name: The name of the table in the online feature store.
        """
        super().__init__(
            name=name,
            system_name="memory",
            table_uri={"table_name": table_name},
            keys=keys,
            timestamp_field=None,
            timestamp_format="epoch",
        )
        self.table_name = table_name

    def build(
        self,
        registry: "Registry",
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> TableDescriptor:
        built_table = MemoryStoreSource(
            name=self.name, keys=self.keys, table_name=self.table_name
        )
        built_table.schema = (
            MemoryOnlineStore.get_instance().table_infos.get(self.table_name).schema
        )
        return built_table

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "keys": self.keys,
            "table_name": self.table_name,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "MemoryStoreSource":
        return MemoryStoreSource(
            name=json_dict["name"],
            keys=json_dict["keys"],
            table_name=json_dict["table_name"],
        )

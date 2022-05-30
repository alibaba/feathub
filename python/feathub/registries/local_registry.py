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

from typing import List, Dict

from feathub.table.table_descriptor import TableDescriptor
from feathub.registries.registry import Registry


class LocalRegistry(Registry):
    """
    A registry that stores entities in memory.
    """

    REGISTRY_TYPE = "local"

    def __init__(self, config: Dict):
        """
        :param config: The registry configuration.
        """
        super().__init__()
        self.config = config
        self.namespace = config.get("namespace", "default")
        self.tables = {}

    # TODO: persist metadata on disks if cache_only == True.
    # TODO: maintain the version and version_timestamp so that we can recover the
    # lineage information of a table as upstream table evolves.
    def build_features(
        self, features_list: List[TableDescriptor]
    ) -> List[TableDescriptor]:
        result = []
        for table in features_list:
            self.tables[table.name] = table.build(self)
            result.append(self.tables[table.name])

        return result

    # TODO: implement this method.
    def register_features(self, features: TableDescriptor, override=True) -> bool:
        raise RuntimeError("operation not supported")

    def get_features(self, name) -> TableDescriptor:
        if name not in self.tables:
            raise RuntimeError(f"Table '{name}' is not found in the cache or registry.")
        return self.tables.get(name)

    # TODO: implement this method.
    def delete_features(self, name) -> bool:
        raise RuntimeError("operation not supported")

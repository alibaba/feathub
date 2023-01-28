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

from typing import List, Dict, Any, Optional

from feathub.common.config import ConfigDef
from feathub.common.exceptions import FeathubException
from feathub.registries.registry import Registry
from feathub.registries.registry_config import RegistryConfig, REGISTRY_PREFIX
from feathub.table.table_descriptor import TableDescriptor

LOCAL_REGISTRY_PREFIX = REGISTRY_PREFIX + "local."

NAMESPACE_CONFIG = LOCAL_REGISTRY_PREFIX + "namespace"
NAMESPACE_DOC = "The namespace of the local registry."

local_registry_config_defs = [
    ConfigDef(
        name=NAMESPACE_CONFIG,
        value_type=str,
        description=NAMESPACE_DOC,
        default_value="default",
    )
]


class LocalRegistryConfig(RegistryConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(local_registry_config_defs)


class LocalRegistry(Registry):
    """
    A registry that stores entities in memory.
    """

    REGISTRY_TYPE = "local"

    def __init__(self, props: Dict) -> None:
        """
        :param props: The registry properties.
        """
        super().__init__(LocalRegistry.REGISTRY_TYPE, props)
        local_registry_config = LocalRegistryConfig(props)
        self.namespace = local_registry_config.get(NAMESPACE_CONFIG)
        self.tables: Dict[str, TableDescriptor] = {}

    # TODO: persist metadata on disks if cache_only == True.
    # TODO: maintain the version and version_timestamp so that we can recover the
    # lineage information of a table as upstream table evolves.
    def build_features(
        self, features_list: List[TableDescriptor], props: Optional[Dict] = None
    ) -> List[TableDescriptor]:
        result = []
        for table in features_list:
            if table.name == "":
                raise FeathubException(
                    "Cannot build a TableDescriptor with empty name."
                )
            self.tables[table.name] = table.build(self, props)
            result.append(self.tables[table.name])

        return result

    def register_features(
        self, features: TableDescriptor, override: bool = True
    ) -> bool:
        if features.name == "":
            raise FeathubException("Cannot register a TableDescriptor with empty name.")
        if features.name in self.tables and not override:
            return False
        self.tables[features.name] = features
        return True

    def get_features(self, name: str) -> TableDescriptor:
        if name not in self.tables:
            raise RuntimeError(
                f"Table '{name}' is not found in the cache or registry. "
                "Please invoke build_features(..) for this table."
            )
        return self.tables[name]

    def delete_features(self, name: str) -> bool:
        if name not in self.tables:
            return False
        self.tables.pop(name)
        return True

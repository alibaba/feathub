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

from typing import List, Dict, Any, Optional, Tuple

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
        self.tables: Dict[str, Tuple[TableDescriptor, TableDescriptor]] = {}

    # TODO: maintain the version and version_timestamp so that we can recover the
    # lineage information of a table as upstream table evolves.
    def build_features(
        self,
        feature_descriptors: List[TableDescriptor],
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> List[TableDescriptor]:
        result = []
        for table in feature_descriptors:
            if table.name == "":
                raise FeathubException(
                    "Cannot build a TableDescriptor with empty name."
                )
            self.tables[table.name] = (
                table,
                table.build(self, force_update=force_update, props=props),
            )
            result.append(self.tables[table.name][1])

        return result

    def register_features(
        self, feature_descriptors: List[TableDescriptor], force_update: bool = False
    ) -> List[bool]:
        self.build_features(feature_descriptors, force_update)
        return [True for _ in feature_descriptors]

    def get_features(
        self, name: str, force_update: bool = False, is_resolved: bool = True
    ) -> TableDescriptor:
        if name not in self.tables:
            raise RuntimeError(
                f"Table '{name}' is not found in the cache or registry. "
                "Please invoke build_features(..) for this table."
            )
        return self.tables[name][1 if is_resolved else 0]

    def delete_features(self, name: str) -> bool:
        if name not in self.tables:
            return False
        self.tables.pop(name)
        return True

    def clear_features(self) -> None:
        """
        Deletes all features ever registered into this registry.
        """
        self.tables.clear()

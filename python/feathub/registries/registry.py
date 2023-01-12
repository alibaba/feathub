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

from __future__ import annotations
from typing import List, Dict, Optional
from abc import ABC, abstractmethod

from feathub.registries.registry_config import (
    RegistryConfig,
    REGISTRY_TYPE_CONFIG,
    RegistryType,
)
from feathub.table.table_descriptor import TableDescriptor


class Registry(ABC):
    """
    A registry implements APIs to build, register, get, and delete table descriptors,
    such as feature views with feature transformation definitions.
    """

    def __init__(self, registry_type: str, config: Dict) -> None:
        """
        :param registry_type: The type of the registry
        :param config: The registry configuration.
        """
        self._registry_type = registry_type
        self._config = config

    @abstractmethod
    def build_features(
        self, features_list: List[TableDescriptor], props: Optional[Dict] = None
    ) -> List[TableDescriptor]:
        """
        For each table descriptor in the given list, resolve this descriptor by
        recursively replacing its dependent table and feature names with the
        corresponding table descriptors and features from the cache or registry.
        Then recursively configure the table descriptor and its dependent table that is
        referred by a TableDescriptor with the given global properties if it is not
        configured already.

        And caches the resolved descriptors and well as their dependent table
        descriptors in memory so that they can be used when building other table
        descriptors.

        :param features_list: A list of table descriptors.
        :param props: Optional. If it is not None, it is the global properties that are
                      used to configure the given table descriptors.
        :return: A list of resolved descriptors corresponding to the input descriptors.
        """
        pass

    @abstractmethod
    def register_features(
        self, features: TableDescriptor, override: bool = True
    ) -> bool:
        """
        Registers the given table descriptor in the registry. Each descriptor is
        uniquely identified by its name in the registry.

        :param features: A table descriptor to be registered.
        :param override: Indicates whether the registration can overwrite existing
                         descriptor or not.
        :return: True iff registration is successful.
        """
        pass

    @abstractmethod
    def get_features(self, name: str) -> TableDescriptor:
        """
        Returns the table descriptor previously registered with the given name. Raises
        RuntimeError if there is no such table in the registry.

        :param name: The name of the table descriptor to search for.
        :return: A table descriptor with the given name.
        """
        pass

    @abstractmethod
    def delete_features(self, name: str) -> bool:
        """
        Deletes the table descriptor with the given name from the registry.

        :param name: The name of the table descriptor to be deleted.
        :return: True iff a table descriptor with the given name is deleted.
        """
        pass

    @staticmethod
    def instantiate(props: Dict) -> Registry:
        """
        Instantiates a registry using the given properties.
        """

        registry_config = RegistryConfig(props)
        registry_type = RegistryType(registry_config.get(REGISTRY_TYPE_CONFIG))

        if registry_type == RegistryType.LOCAL:
            from feathub.registries.local_registry import LocalRegistry

            return LocalRegistry(props=props)

        raise RuntimeError(f"Failed to instantiate registry with props={props}.")

    @property
    def config(self) -> Dict:
        return self._config

    @property
    def registry_type(self) -> str:
        return self._registry_type

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self._config == other._config
            and self._registry_type == other._registry_type
        )

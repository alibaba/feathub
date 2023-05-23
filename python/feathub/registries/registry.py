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

    # TODO: move :param props: to other place to avoid job-specific global
    #  properties affecting feature descriptors saved in Registry.
    @abstractmethod
    def build_features(
        self,
        feature_descriptors: List[TableDescriptor],
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> List[TableDescriptor]:
        """
        For each table descriptor in the given list, resolve this descriptor by
        recursively replacing its dependent table and feature names with the
        corresponding table descriptors and features from the cache or registry.
        Then recursively configure the table descriptor and its dependent table that is
        referred by a TableDescriptor with the given global properties if it is not
        configured already.

        And caches the resolved descriptors as well as their dependent table
        descriptors in memory so that they can be used when building other table
        descriptors.

        :param feature_descriptors: A list of table descriptors.
        :param force_update: If True, the feature descriptor would be directly searched
                             in registry. If False, the feature descriptor would be
                             searched in local cache first.
        :param props: Optional. If it is not None, it is the global properties that are
                      used to configure the given table descriptors.
        :return: A list of resolved descriptors corresponding to the input descriptors.
        """
        pass

    @abstractmethod
    def register_features(
        self, feature_descriptors: List[TableDescriptor], force_update: bool = False
    ) -> List[bool]:
        """
        Registers the given table descriptor in the registry after building and
        caching them in memory as described in build_features. Each descriptor is
        uniquely identified by its name in the registry.

        :param feature_descriptors: A table descriptor to be registered.
        :param force_update: If True, the feature descriptor would be directly searched
                             in registry. If False, the feature descriptor would be
                             searched in local cache first.
        :return: A list of bool values denoting whether the registration for each
                 descriptor is successful.
        """
        pass

    @abstractmethod
    def get_features(
        self, name: str, force_update: bool = False, is_resolved: bool = True
    ) -> TableDescriptor:
        """
        Returns the table descriptor previously registered with the given name. Raises
        RuntimeError if there is no such table in the registry.

        :param name: The name of the table descriptor to search for.
        :param force_update: If True, the feature descriptor would be directly searched
                             in registry. If False, the feature descriptor would be
                             searched in local cache first.
        :param is_resolved: If False, the original feature descriptor would be returned.
                            If True, the descriptor that had been resolved would be
                            returned.
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
        elif registry_type == RegistryType.MYSQL:
            from feathub.registries.mysql_registry import MySqlRegistry

            return MySqlRegistry(props=props)

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

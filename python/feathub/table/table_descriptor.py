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

from abc import abstractmethod
from typing import Optional, List, TYPE_CHECKING, Dict

from feathub.common.exceptions import FeathubException
from feathub.feature_views.feature import Feature
from feathub.registries.entity import Entity

if TYPE_CHECKING:
    from feathub.registries.registry import Registry


class TableDescriptor(Entity):
    """
    Provides metadata to access, derive and interpret a table of feature values. Each
    column of the table corresponds to a feature.

    A TableDescriptor is uniquely identified by its name in the feature registry. Its
    interpretation is agnostic to any processor type.

    TableDescriptors use `timestamp_field` and `timestamp_format` to define the time
    attribute of each row of feature value. Valid field values and formats are as
    follows.

    - If the format value is "epoch", the timestamp field should contain numeric values
      representing unix timestamps in seconds.
    - If the format value is "epoch_millis", The timestamp field should contain numeric
      values representing unix timestamps in milliseconds.
    - Otherwise, the format value should match the requirement of the C standard
      (1989 version). For example, the default valid value is "%Y-%m-%d %H:%M:%S". The
      timestamp field should contain string values in the specified format. See Python's
      strftime() and strptime() Behavior for a brief list of and introduction to these
      format patterns.
      https://docs.python.org/3.7/library/datetime.html#strftime-strptime-behavior
    """

    def __init__(
        self,
        name: str,
        keys: Optional[List[str]],
        timestamp_field: Optional[str],
        timestamp_format: str = "epoch",
    ) -> None:
        """
        :param name: The unique identifier of this feature view in the registry.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field. Only effective when
                                 `timestamp_field` is not None.
        """
        super().__init__()
        self.name = name
        self.keys = keys
        self.timestamp_field = timestamp_field
        self.timestamp_format = timestamp_format

    def build(
        self,
        registry: "Registry",
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> TableDescriptor:
        """
        Gets a copy of self after recursively replacing the dependent table and feature
        names with the corresponding table descriptors and features, then
        recursively configuring the table descriptor and its dependent table with the
        given global properties if it is not configured already.

        And caches this descriptor as well as its dependent table descriptors in memory
        so that they can be used when building other table descriptors.

        :param registry: The entity registry to retrieve table description by name.
        :param force_update: If True, the feature descriptor would be directly searched
                             in registry. If False, the feature descriptor would be
                             searched in local cache first.
        :param props: Optional. If it is not None, it is the global properties that are
                      used to configure the given table descriptors.
        :return: A resolved table descriptor.
        """

        # TODO: return a COPY of self instead of the existing Python object.
        return self

    def get_feature(self, feature_name: str) -> Feature:
        """
        Returns the feature whose name matches the given feature name.

        :param feature_name: The name of the feature to look for.
        :return: The feature with the given name.
        """
        for feature in self.get_output_features():
            if feature_name == feature.name:
                return feature

        raise FeathubException(
            f"Failed to find the feature '{feature_name}' in {self.to_json()}."
        )

    @abstractmethod
    def get_output_features(self) -> List[Feature]:
        """
        Return a list of output features of the table.
        """
        pass

    @abstractmethod
    def get_bounded_view(self) -> TableDescriptor:
        """
        If the Table is bounded, returns self. Otherwise, return a copy of self that is
        bounded.

        :return: A bounded table descriptor.
        """
        pass

    @abstractmethod
    def is_bounded(self) -> bool:
        """
        Whether the Table is bounded.
        """
        pass

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

from typing import Optional, List, Union
from collections import OrderedDict

from feathub.table.table import TableDescriptor
from feathub.feature_views.feature import Feature


class FeatureView(TableDescriptor):
    """
    Provides metadata to derive a table of feature values from other tables.
    """

    def __init__(
        self,
        name: str,
        source: Union[str, TableDescriptor],
        features: List[Union[str, Feature]],
        keep_source_fields: bool = False,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param source: The source dataset used to derive this feature view. If it is a
                       string, it should refer to the name of a table descriptor in the
                       registry.
        :param features: A list of features to be joined onto this feature view.
                         If a feature is a string, it should be either in the format
                         {table_name}.{feature_name}, which refers to a feature in the
                         table with the given name, or in the format {feature_name},
                         which refers to a feature in the source table.
        :param keep_source_fields: True iff all fields in the source table should be
                                   included in this table.
        """

        self.source = source
        self.features = features
        self.keep_source_fields = keep_source_fields

        is_unresolved = self.is_unresolved()
        keys = None if is_unresolved else self._get_keys()
        if keys is not None:
            # Uses table's keys as features' keys if features' keys are not specified.
            for feature in [f for f in features if f.keys is None]:
                feature.keys = keys
        super().__init__(
            name=name,
            keys=keys,
            timestamp_field=None if is_unresolved else source.timestamp_field,
            timestamp_format=None if is_unresolved else source.timestamp_format,
        )

    def is_unresolved(self) -> bool:
        return isinstance(self.source, str) or any(
            isinstance(f, str) for f in self.features
        )

    def get_output_fields(self, source_fields: List[str]) -> List[str]:
        """
        Returns the names of fields of this table descriptor.
        The output fields include:
        - All fields in the source_fields if keep_source_fields is True.
        - The timestamp field if it is not None.
        - All features and features' keys.

        :param source_fields: The names of fields of the source table.
        :return: The names of fields of this table descriptor.
        """
        output_fields = []
        if self.keep_source_fields:
            output_fields.extend(source_fields)
        elif self.timestamp_field is not None:
            output_fields.append(self.timestamp_field)

        for feature in self.features:
            if feature.keys is not None:
                output_fields.extend(feature.keys)
            output_fields.append(feature.name)

        # Order output fields similar to their order in the source table.
        reordered_output_fields = []
        for field in source_fields:
            if field in output_fields:
                reordered_output_fields.append(field)
        reordered_output_fields.extend(output_fields)

        return list(OrderedDict.fromkeys(reordered_output_fields))

    def get_feature(self, feature_name: str) -> Feature:
        if self.is_unresolved():
            raise RuntimeError("Build this feature view before getting features.")
        for feature in self.features:
            if feature_name == feature.name:
                return feature

        if not self.keep_source_fields:
            raise RuntimeError(
                f"Failed to find the feature '{feature_name}' in {self.to_json()}."
            )
        return self.source.get_feature(feature_name)

    def _get_keys(self) -> Optional[List[str]]:
        if self.keep_source_fields and self.source.keys is None:
            return None

        feature_with_keys = [f for f in self.features if f.keys is not None]
        # Table's keys are unknown if no feature has keys specified.
        if not self.keep_source_fields and not feature_with_keys:
            return None

        key_fields = []
        if self.keep_source_fields:
            key_fields.extend(self.source.keys)
        for feature in feature_with_keys:
            key_fields.extend(feature.keys)

        return list(OrderedDict.fromkeys(key_fields))

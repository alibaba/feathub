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
from typing import List, Union

from feathub.table.table_descriptor import TableDescriptor
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.registries.registry import Registry


class DerivedFeatureView(FeatureView):
    """
    Derives features by applying the given transformations on an existing table.

    Supports per-row transformation and window aggregation transformation. Does not
    support table join.
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
        :param features: A list of features to be computed from the source table. If a
                         feature is a string, it should refer to a feature name in the
                         source table.
        :param keep_source_fields: True iff all fields in the source table should be
                                   included in this table.
        """
        super().__init__(
            name=name,
            source=source,
            features=features,
            keep_source_fields=keep_source_fields,
        )

    def build(self, registry: Registry) -> TableDescriptor:
        """
        Gets a copy of self as a resolved table descriptor.

        The source and features might be strings that reference table name and
        field names respectively. The references are replaced with the corresponding
        table descriptors and features.

        The source table descriptor will be cached in the registry.
        """
        if isinstance(self.source, str):
            source = registry.get_features(name=self.source)
        else:
            source = registry.build_features(features_list=[self.source])[0]

        features = []
        for feature in self.features:
            if isinstance(feature, str):
                feature = source.get_feature(feature_name=feature)
            features.append(feature)

        return DerivedFeatureView(
            name=self.name,
            source=source,
            features=features,
            keep_source_fields=self.keep_source_fields,
        )

    def to_json(self):
        return {
            "type": "DerivedFeatureView",
            "name": self.name,
            "source": (
                self.source if isinstance(self.source, str) else self.source.to_json()
            ),
            "features": [
                feature if isinstance(feature, str) else feature.to_json()
                for feature in self.features
            ],
            "keep_source_fields": self.keep_source_fields,
        }

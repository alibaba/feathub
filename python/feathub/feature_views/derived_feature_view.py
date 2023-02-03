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
from typing import Union, Dict, Sequence, Optional

from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.table.table_descriptor import TableDescriptor
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.registries.registry import Registry


class DerivedFeatureView(FeatureView):
    """
    Derives features by applying the given transformations on an existing table.

    Supports per-row transformation, over window aggregation transformation and table
    join.
    """

    def __init__(
        self,
        name: str,
        source: Union[str, TableDescriptor],
        features: Sequence[Union[str, Feature]],
        keep_source_fields: bool = False,
        filter_expr: Optional[str] = None,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param source: The source dataset used to derive this feature view. If it is a
                       string, it should refer to the name of a table descriptor in the
                       registry.
        :param features: A list of features to be computed from the source table. If a
                         feature is a string, it should be either in the format
                         {table_name}.{feature_name}, which refers to a feature in the
                         table with the given name, or in the format {feature_name},
                         which refers to a feature in the source table. For any feature
                         computed by ExpressionTransform or OverWindowTransform, its
                         expression can only depend on the features specified earlier in
                         this list and the features in the source table.
        :param keep_source_fields: True iff all fields in the source table should be
                                   included in this table.
        :param filter_expr: Optional. If it is not None, it represents a Feathub
                            expression which evaluates to a boolean value. The filter
                            expression is evaluated after other transformations in the
                            feature view, and only those rows which evaluate to True
                            will be outputted by the feature view.
        """
        super().__init__(
            name=name,
            source=source,
            features=features,
            keep_source_fields=keep_source_fields,
        )
        self.filter_expr = filter_expr

    def build(
        self, registry: Registry, props: Optional[Dict] = None
    ) -> TableDescriptor:
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
                parts = feature.split(".")
                if len(parts) == 2:
                    join_table_name = parts[0]
                    join_feature_name = parts[1]
                else:
                    join_table_name = source.name
                    join_feature_name = parts[0]

                table_desc = registry.get_features(name=join_table_name)
                join_feature = table_desc.get_feature(feature_name=join_feature_name)
                if source.name == join_table_name:
                    feature = join_feature
                elif join_feature.keys is not None:
                    feature = Feature(
                        name=join_feature_name,
                        dtype=join_feature.dtype,
                        transform=JoinTransform(join_table_name, join_feature_name),
                        keys=join_feature.keys,
                    )
                else:
                    raise RuntimeError(
                        f"Feature '{join_feature_name}' in the remote table "
                        f"'{join_table_name}' does not have keys specified."
                    )
            features.append(feature)

        # TODO: Validate ExpressionTransform features that depends on
        #  OverWindowTransform or JoinTransform features are listed after the first
        #  OverWindowTransform or JoinTransform feature.

        return DerivedFeatureView(
            name=self.name,
            source=source,
            features=features,
            keep_source_fields=self.keep_source_fields,
            filter_expr=self.filter_expr,
        )

    def to_json(self) -> Dict:
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
            "filter_expr": self.filter_expr,
        }

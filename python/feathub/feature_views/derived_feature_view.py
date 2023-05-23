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

from typing import Union, Dict, Sequence, Optional, List

from feathub.common.exceptions import FeathubException
from feathub.common.utils import from_json, append_metadata_to_json
from feathub.dsl.expr_utils import get_variables
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


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
        :param keep_source_fields: True iff all features in the source table should be
                                   included in this table. The feature in the source
                                   will be overwritten by the feature in this feature
                                   view if they have the same name.
        :param filter_expr: Optional. If it is not None, it represents a FeatHub
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
        self,
        registry: "Registry",
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> TableDescriptor:
        """
        Gets a copy of self as a resolved table descriptor.

        The source and features might be strings that reference table name and
        field names respectively. The references are replaced with the corresponding
        table descriptors and features.

        The source table descriptor will be cached in the registry.
        """
        if isinstance(self.source, str):
            source = registry.get_features(name=self.source, force_update=force_update)
        else:
            source = registry.build_features(
                feature_descriptors=[self.source], force_update=force_update
            )[0]

        features = []
        for feature in self.features:
            if isinstance(feature, str):
                feature = self._get_feature_from_feature_str(
                    feature, registry, source, force_update
                )
            features.append(feature)

        self._validate(features, source)

        return DerivedFeatureView(
            name=self.name,
            source=source,
            features=features,
            keep_source_fields=self.keep_source_fields,
            filter_expr=self.filter_expr,
        )

    @staticmethod
    def _validate(features: List[Feature], source: TableDescriptor) -> None:
        # Check if the feature only depends on the features specified earlier or the
        # features in the source table.
        valid_variables = set([f.name for f in source.get_output_features()])
        for feature in features:
            transform = feature.transform
            if isinstance(transform, JoinTransform):
                variables = set()
            elif isinstance(transform, PythonUdfTransform):
                variables = {f.name for f in feature.input_features}
            elif isinstance(transform, OverWindowTransform):
                variables = {
                    *(
                        get_variables(transform.filter_expr)
                        if transform.filter_expr is not None
                        else set()
                    ),
                    *get_variables(transform.expr),
                    *transform.group_by_keys,
                }
            elif isinstance(transform, ExpressionTransform):
                variables = get_variables(transform.expr)
            else:
                raise FeathubException(
                    f"Unexpected transform {transform} of feature {feature.name} in "
                    f"DerivedFeatureView."
                )

            if not variables.issubset(valid_variables):
                raise FeathubException(
                    f"Feature {feature} should only depend on features specified "
                    f"earlier or the features in the source table."
                )
            valid_variables.add(feature.name)

    @staticmethod
    def _get_feature_from_feature_str(
        feature_str: str,
        registry: Registry,
        source: TableDescriptor,
        force_update: bool,
    ) -> Feature:
        parts = feature_str.split(".")
        if len(parts) == 1:
            source_feature = source.get_feature(parts[0])
            feature = Feature(
                name=source_feature.name,
                dtype=source_feature.dtype,
                transform=ExpressionTransform(f"`{source_feature.name}`"),
                keys=source_feature.keys,
            )
            return feature
        elif len(parts) == 2:
            join_table_name = parts[0]
            join_feature_name = parts[1]
            table_desc = registry.get_features(
                name=join_table_name, force_update=force_update
            )
            join_feature = table_desc.get_feature(feature_name=join_feature_name)
            if join_feature.keys is None:
                raise RuntimeError(
                    f"Feature '{join_feature_name}' in the remote table "
                    f"'{join_table_name}' does not have keys specified."
                )
            return Feature(
                name=join_feature_name,
                dtype=join_feature.dtype,
                transform=JoinTransform(join_table_name, join_feature_name),
                keys=join_feature.keys,
            )
        else:
            raise FeathubException(
                "Invalid string format. If a feature is a string, it should be either "
                "in the format {table_name}.{feature_name} or in the "
                "format {feature_name}."
            )

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
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

    @classmethod
    def from_json(cls, json_dict: Dict) -> "DerivedFeatureView":
        return DerivedFeatureView(
            name=json_dict["name"],
            source=json_dict["source"]
            if isinstance(json_dict["source"], str)
            else from_json(json_dict["source"]),
            features=[
                feature if isinstance(feature, str) else from_json(feature)
                for feature in json_dict["features"]
            ],
            keep_source_fields=json_dict["keep_source_fields"],
            filter_expr=json_dict["filter_expr"],
        )

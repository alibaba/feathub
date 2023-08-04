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

from typing import Union, Dict, Sequence, Optional, List, cast

from feathub.common.exceptions import FeathubException
from feathub.common.types import DType
from feathub.common.utils import from_json, append_metadata_to_json
from feathub.dsl.ast import BracketOp
from feathub.dsl.expr_parser import ExprParser
from feathub.dsl.expr_utils import get_variables, is_id, is_static_map_lookup_op
from feathub.feature_views.feature import Feature, get_default_feature_name
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.java_udf_transform import JavaUdfTransform
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


_parser = ExprParser()


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
        keep_source_metrics: bool = False,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param source: The source dataset used to derive this feature view. If it is a
                       string, it should refer to the name of a table descriptor in the
                       registry.
        :param features: A list of features to be computed in this feature view. If a
                         feature is a string, it should be in either of the following
                         formats:
                         1. {feature_name}, which refers to a feature in the table with
                            the given name
                         2. {table_name}.{feature_name}, which refers to a feature in
                            the source table
                         3. {table_name}.{map_feature_name}[{literal_key_value}], which
                            refers to a static lookup of a map feature in the table with
                            the given name
                         For any feature computed by ExpressionTransform or
                         OverWindowTransform, its expression can only depend on the
                         features specified earlier in this list and the features in the
                         source table.
        :param keep_source_fields: True iff all features in the source table should be
                                   included in this table. The feature in the source
                                   will be overwritten by the feature in this feature
                                   view if they have the same name.
        :param filter_expr: Optional. If it is not None, it represents a FeatHub
                            expression which evaluates to a boolean value. The filter
                            expression is evaluated after other transformations in the
                            feature view, and only those rows which evaluate to True
                            will be outputted by the feature view.
        :param keep_source_metrics: If it is true and this feature view is materialized
                                    to a sink, FeatHub will recursively enumerate source
                                    feature view of this and every upstream feature
                                    view whose keep_source_fields == true, and report
                                    metrics defined in those feature views.
        """
        if any(
            isinstance(feature, Feature)
            and isinstance(feature.transform, JavaUdfTransform)
            for feature in features
        ):
            if len(features) > 1:
                raise FeathubException(
                    "JavaUdfTransform cannot be used with other transformations."
                )
            if keep_source_fields or filter_expr:
                raise FeathubException(
                    "JavaUdfTransform cannot be used with keeping source fields or "
                    "filter expression."
                )

        super().__init__(
            name=name,
            source=source,
            features=features,
            keep_source_fields=keep_source_fields,
            keep_source_metrics=keep_source_metrics,
        )
        self.filter_expr = filter_expr

    def get_output_fields(self, source_fields: List[str]) -> List[str]:
        if (
            len(self.features) == 1
            and isinstance(self.features[0], Feature)
            and isinstance(self.features[0].transform, JavaUdfTransform)
        ):
            return self.features[0].transform.schema.field_names  # type: ignore
        return super(DerivedFeatureView, self).get_output_fields(source_fields)

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

        existing_feature_names = {x.name for x in source.get_output_features()}

        features = []
        for feature in self.features:
            if isinstance(feature, str):
                feature = self._get_feature_from_feature_str(
                    feature,
                    registry,
                    source,
                    force_update,
                    get_default_feature_name(existing_feature_names),
                )
                existing_feature_names.add(feature.name)
            features.append(feature)

        self._validate(features, source)

        return DerivedFeatureView(
            name=self.name,
            source=source,
            features=features,
            keep_source_fields=self.keep_source_fields,
            filter_expr=self.filter_expr,
            keep_source_metrics=self.keep_source_metrics,
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
        default_feature_name: str,
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
            table_desc = registry.get_features(
                name=join_table_name, force_update=force_update
            )
            if is_id(parts[1]):
                join_feature_name = parts[1]
                join_feature = table_desc.get_feature(feature_name=join_feature_name)
                if join_feature.keys is None:
                    raise RuntimeError(
                        f"Feature '{join_feature_name}' in the remote table "
                        f"'{join_table_name}' does not have keys specified."
                    )
                return Feature(
                    name=join_feature_name,
                    dtype=join_feature.dtype,
                    transform=JoinTransform(join_table_name, f"`{join_feature_name}`"),
                    keys=join_feature.keys,
                )
            elif is_static_map_lookup_op(parts[1]):
                join_feature_expr = parts[1]
                variable_types: Dict[str, Optional[DType]] = {
                    f.name: f.dtype for f in table_desc.get_output_features()
                }
                feature_type = cast(
                    BracketOp, _parser.parse(join_feature_expr)
                ).right_child.eval_dtype(variable_types)
                return Feature(
                    name=default_feature_name,
                    dtype=feature_type,
                    transform=JoinTransform(join_table_name, join_feature_expr),
                    keys=table_desc.keys,
                )

        raise FeathubException(
            "Invalid string format. If a feature is a string, it should be in either "
            "of the following formats."
            " 1. {feature_name}"
            " 2. {table_name}.{feature_name}"
            " 3. {table_name}.{map_feature_name}[{literal_key_value}]"
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
            "keep_source_metrics": self.keep_source_metrics,
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
            keep_source_metrics=json_dict["keep_source_metrics"],
        )

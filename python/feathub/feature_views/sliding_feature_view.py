#  Copyright 2022 The Feathub Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from typing import Dict, Union, Sequence, Set

from feathub.common.exceptions import FeathubException
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class SlidingFeatureView(FeatureView):
    """
    Derives features by applying sliding window transformations on an existing table.

    Supports per-row expression transformation and sliding window transformation. Does
    not support table join or over window transformation. Those features defined with
    expression transformation must be part of the groups keys of sliding window
    transformations used in this feature view. And sliding window transformations used
    in this feature view must have the same step size, window size and group-by keys.

    Unlike DerivedFeatureView, the number of rows emitted by SlidingFeatureView might
    not equal the number of rows in its source table. This is because the features
    defined by a sliding window transformation can change over time even if there is no
    change in the source table.
    """

    def __init__(
        self,
        name: str,
        source: Union[str, TableDescriptor],
        features: Sequence[Union[str, Feature]],
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param source: The source dataset used to derive this feature view. If it is a
                       string, it should refer to the name of a table descriptor in the
                       registry.
        :param features: A list of features to be computed from the source table. The
                         feature should be computed by either ExpressionTransform or
                         SlidingWindowTransform. It must have at least one feature with
                         SlidingWindowTransform. If a feature is computed by
                         ExpressionTransform it should be used as one of the grouping
                         keys of SlidingWindowTransforms. If a feature is a string, it
                         should refer to a feature name in the source table, and it
                         should be used as one of the grouping key of
                         SlidingWindowTransforms. All the SlidingWindowTransforms should
                         have the same step_size, window size and group-by key.
        """
        super().__init__(
            name=name,
            source=source,
            features=features,
            keep_source_fields=False,
        )

        self._validate(features)

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

        return SlidingFeatureView(name=self.name, source=source, features=features)

    def to_json(self) -> Dict:
        return {
            "type": "SlidingFeatureView",
            "name": self.name,
            "source": (
                self.source if isinstance(self.source, str) else self.source.to_json()
            ),
            "features": [
                feature if isinstance(feature, str) else feature.to_json()
                for feature in self.features
            ],
        }

    @staticmethod
    def _validate(features: Sequence[Union[str, Feature]]) -> None:
        sliding_window_transforms = [
            feature.transform
            for feature in features
            if isinstance(feature, Feature)
            and isinstance(feature.transform, SlidingWindowTransform)
        ]
        if len(sliding_window_transforms) < 1:
            raise FeathubException(
                "SlidingWindowFeatureView must have at least one feature with "
                "SlidingWindowTransform."
            )

        expression_feature_names: Set[str] = set()
        sliding_window_transforms_group_by_keys: Set[str] = set()
        for feature in features:
            if isinstance(feature, str):
                expression_feature_names.add(feature)
                continue

            transform = feature.transform
            if isinstance(transform, ExpressionTransform):
                expression_feature_names.add(feature.name)
            elif isinstance(transform, SlidingWindowTransform):
                sliding_window_transforms_group_by_keys.update(transform.group_by_keys)
            else:
                raise FeathubException(
                    f"Feature '{feature.name}' uses unsupported transform type "
                    f"'{type(transform)}'."
                )

        invalid_expression_feature_names = set()
        for expression_feature_name in expression_feature_names:
            if expression_feature_name not in sliding_window_transforms_group_by_keys:
                invalid_expression_feature_names.add(expression_feature_name)
        if len(invalid_expression_feature_names) != 0:
            raise FeathubException(
                f"{invalid_expression_feature_names} are not used as grouping key of "
                f"the sliding windows."
            )

        group_by_keys = set(
            [tuple(transform.group_by_keys) for transform in sliding_window_transforms]
        )
        if len(group_by_keys) > 1:
            raise FeathubException(
                f"SlidingWindowTransforms have different group-by keys "
                f"{group_by_keys}."
            )

        steps = set([transform.step_size for transform in sliding_window_transforms])
        if len(steps) > 1:
            raise FeathubException(
                f"SlidingWindowTransforms have different step size " f"{steps}."
            )

        window_sizes = set(
            [transform.window_size for transform in sliding_window_transforms]
        )
        if len(window_sizes) > 1:
            raise FeathubException(
                f"SlidingWindowTransforms have different window size "
                f"{window_sizes}."
            )

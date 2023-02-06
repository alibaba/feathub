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
from typing import Dict, Union, Sequence, Set, Any, Optional

from feathub.common import types
from feathub.common.config import BaseConfig, ConfigDef
from feathub.common.exceptions import FeathubException, FeathubConfigurationException
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor

SLIDING_FEATURE_VIEW_PREFIX = "sdk.sliding_feature_view."

ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG = (
    SLIDING_FEATURE_VIEW_PREFIX + "enable_empty_window_output"
)
ENABLE_EMPTY_WINDOW_OUTPUT_DOC = (
    "If it is True, when the sliding window becomes emtpy, it outputs zero value for "
    "aggregation function SUM and COUNT, and output None for other aggregation "
    "function. If it is False, the sliding window doesn't output anything when the "
    "sliding window becomes empty."
)

SKIP_SAME_WINDOW_OUTPUT_CONFIG = SLIDING_FEATURE_VIEW_PREFIX + "skip_same_window_output"
SKIP_SAME_WINDOW_OUTPUT_DOC = (
    "If it is True, the sliding feature view only outputs when the result of the "
    "sliding window changes. If it is False, the sliding feature view outputs at every "
    "step size even if the result of the sliding window doesn't change."
)

sliding_feature_view_config_defs = [
    ConfigDef(
        name=ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
        value_type=bool,
        description=ENABLE_EMPTY_WINDOW_OUTPUT_DOC,
        default_value=True,
    ),
    ConfigDef(
        name=SKIP_SAME_WINDOW_OUTPUT_CONFIG,
        value_type=bool,
        description=SKIP_SAME_WINDOW_OUTPUT_DOC,
        default_value=True,
    ),
]


class SlidingFeatureViewConfig(BaseConfig):
    def __init__(self, original_props: Dict[str, Any]) -> None:
        super().__init__(original_props)
        self.update_config_values(sliding_feature_view_config_defs)


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

    The output fields of this feature view consist of the features explicitly listed in
    the `features` parameter, their group-by keys, and an additional field "window_time"
    that represents the timestamp when the sliding window closed with default format
    "epoch". Other fields in the source table will not be included.

    The SlidingFeatureView can be configured with the following global configuration:
    - sdk.sliding_feature_view.enable_empty_window_output: Default to True. If it is
      True, when the sliding window becomes emtpy, it outputs zero value for aggregation
      function SUM and COUNT, and output None for other aggregation function. If it is
      False, the sliding window doesn't output anything when the sliding window becomes
      empty.
    - sdk.sliding_feature_view.skip_same_window_output: Default to True. If it is True,
      the sliding feature view only outputs when the result of the sliding window
      changes. If it is False, the sliding feature view outputs at every step size even
      if the result of the sliding window doesn't change.

    Note that setting sdk.sliding_feature_view.enable_empty_window_output to False and
    sdk.sliding_feature_view.skip_same_window_output to True is forbidden and an
    exception will be thrown, because, with this setting, we cannot determine if a
    sliding window result is expired or not.
    """

    def __init__(
        self,
        name: str,
        source: Union[str, TableDescriptor],
        features: Sequence[Union[str, Feature]],
        timestamp_field: str = "window_time",
        timestamp_format: str = "epoch_millis",
        filter_expr: Optional[str] = None,
        props: Optional[Dict] = None,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param source: The source dataset used to derive this feature view. If it is a
                       string, it should refer to the name of a table descriptor in the
                       registry.
        :param features: A list of features to be computed from the source table. The
                         feature should be computed by either ExpressionTransform or
                         SlidingWindowTransform. It must have at least one feature with
                         SlidingWindowTransform. Features computed by
                         SlidingWindowTransform must have the same step_size, window
                         size and group-by keys. For any feature computed by
                         ExpressionTransform, it should either be included as one of the
                         group-by keys of SlidingWindowTransform features in this list,
                         or its expression must only depend on the
                         SlidingWindowTransform features specified earlier in this list
                         and their group-by keys.
        :param timestamp_field: The name of the field generated by the
                                SlidingFeatureView that represents the closed window end
                                time for each row.
        :param timestamp_format: The format of the timestamp field.
        :param filter_expr: Optional. If it is not None, it represents a Feathub
                            expression which evaluates to a boolean value. The filter
                            expression is evaluated after other transformations in the
                            feature view, and only those rows which evaluate to True
                            will be outputted by the feature view.
        :param props: Optional. It is not None, it is the properties of the
                      SlidingFeatureView.
        """
        window_time_feature = self._get_window_time_feature(
            timestamp_field, timestamp_format
        )
        super().__init__(
            name=name,
            source=source,
            features=[*features, window_time_feature],
            keep_source_fields=False,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

        self.filter_expr = filter_expr

        self._validate(features)

        self.config = (
            SlidingFeatureViewConfig({})
            if props is None
            else SlidingFeatureViewConfig(props)
        )
        if not self.config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG) and self.config.get(
            SKIP_SAME_WINDOW_OUTPUT_CONFIG
        ):
            raise FeathubConfigurationException(
                "Setting sdk.sliding_feature_view.enable_empty_window_output to False"
                "and sdk.sliding_feature_view.skip_same_window_output to True "
                "is forbidden."
            )

    @staticmethod
    def _get_window_time_feature(
        timestamp_field: str, timestamp_format: str
    ) -> Feature:
        if timestamp_format == "epoch" or timestamp_format == "epoch_millis":
            timestamp_dtype = types.Int64
        else:
            timestamp_dtype = types.String
        window_time_feature = Feature(
            name=timestamp_field,
            dtype=timestamp_dtype,
            transform="CURRENT_EVENT_TIME()",
        )
        return window_time_feature

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
            source = registry.build_features(features_list=[self.source], props=props)[
                0
            ]

        features = []
        for feature in self.features:
            if isinstance(feature, str):
                feature = source.get_feature(feature_name=feature)
            if feature.name == self.timestamp_field:
                continue
            features.append(feature)

        props = {} if props is None else props
        feature_view = SlidingFeatureView(
            name=self.name,
            source=source,
            features=features,
            timestamp_field=self.timestamp_field,
            timestamp_format=self.timestamp_format,
            filter_expr=self.filter_expr,
            props={**props, **self.config.original_props},
        )
        return feature_view

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
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "filter_expr": self.filter_expr,
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

        # Per-row transform features listed before the first SlidingWindowTransform
        # feature.
        per_row_transform_feature_names: Set[str] = set()
        sliding_window_transforms_group_by_keys: Set[str] = set()

        for feature in features:
            if isinstance(feature, str):
                if len(sliding_window_transforms_group_by_keys) == 0:
                    per_row_transform_feature_names.add(feature)
                continue

            transform = feature.transform
            if isinstance(transform, ExpressionTransform):
                if len(sliding_window_transforms_group_by_keys) == 0:
                    per_row_transform_feature_names.add(feature.name)
            elif isinstance(transform, SlidingWindowTransform):
                sliding_window_transforms_group_by_keys.update(transform.group_by_keys)
            elif isinstance(transform, PythonUdfTransform):
                if len(sliding_window_transforms_group_by_keys) == 0:
                    per_row_transform_feature_names.add(feature.name)
            else:
                raise FeathubException(
                    f"Feature '{feature.name}' uses unsupported transform type "
                    f"'{type(transform)}'."
                )

        # TODO: validate that ExpressionTransform feature after the first
        #  SlidingWindowTransform feature depends only on SlidingWindowTransform
        #  features and their group-by keys.
        invalid_expression_feature_names = set()
        for per_row_transform_feature_name in per_row_transform_feature_names:
            if (
                per_row_transform_feature_name
                not in sliding_window_transforms_group_by_keys
            ):
                invalid_expression_feature_names.add(per_row_transform_feature_name)
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

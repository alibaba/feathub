#  Copyright 2022 The FeatHub Authors
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
from typing import Dict, Union, Sequence, Set, Any, Optional, List, OrderedDict, cast

from feathub.common import types
from feathub.common.config import BaseConfig, ConfigDef
from feathub.common.exceptions import FeathubException, FeathubConfigurationException
from feathub.common.utils import from_json, append_metadata_to_json
from feathub.dsl.expr_utils import get_variables
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

WINDOW_TIME_EXPR = "GET_WINDOW_TIME()"

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


# TODO: Add general and formal description for concepts like bounded/unbounded stream,
#  max out of orderness, (watermark,) and late data in Feathub documents.
class SlidingFeatureView(FeatureView):
    """
    Derives features by applying sliding window transformations on an existing table.

    SlidingFeatureView supports ExpressionTransform, PythonUdfTransform, and
    SlidingWindowTransform, but it does not support JoinTransform or
    OverWindowTransform. SlidingWindowTransform used in this feature view must have the
    same step size and group-by keys. Those features defined with ExpressionTransform or
    PythonUdfTransform before the first SlidingWindowTransform feature must be part of
    the group-by keys of sliding window transformations used in this feature view. If
    they are defined after the first SlidingWindowTransform feature, they must only
    depend on the timestamp_field, SlidingWindowTransform features, or the group-by
    keys.

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
        extra_props: Optional[Dict] = None,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param source: The source dataset used to derive this feature view. If it is a
                       string, it should refer to the name of a table descriptor in the
                       registry.
        :param features: A list of features to be computed from the source table. The
                         feature should be computed by either ExpressionTransform,
                         PythonUdfTransform, or SlidingWindowTransform. It must have at
                         least one feature computed by SlidingWindowTransform.
                         Features computed by SlidingWindowTransform must have the same
                         step_size and group-by keys. Features computed by
                         ExpressionTransform or PythonUdfTransform before the first
                         SlidingWindowTransform feature should be included in the
                         group-by keys of SlidingWindowTransform features. If they are
                         listed after the first SlidingWindowTransform feature, they
                         must only depend on the timestamp_field, SlidingWindowTransform
                         features, or the group-by keys.
        :param timestamp_field: The name of the field generated by the
                                SlidingFeatureView that represents the closed window end
                                time for each row. The window end time is the last
                                millisecond included in the window. For example, a
                                one-hour sliding window started at 00:00:00.000 has
                                window end time of 00:59:59.999.
        :param timestamp_format: The format of the timestamp field. See TableDescriptor
                                 for valid format values. Only effective when the
                                 `timestamp_field` is not None.
        :param filter_expr: Optional. If it is not None, it represents a FeatHub
                            expression which evaluates to a boolean value. The filter
                            expression is evaluated after other transformations in the
                            feature view, and only those rows which evaluate to True
                            will be outputted by the feature view.
        :param extra_props: Optional. It is not None, it is the extra properties of the
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

        self.config = (
            SlidingFeatureViewConfig({})
            if extra_props is None
            else SlidingFeatureViewConfig(extra_props)
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
            transform=WINDOW_TIME_EXPR,
        )
        return window_time_feature

    def get_output_fields(self, source_fields: List[str]) -> List[str]:
        if self.is_unresolved():
            raise FeathubException(
                "Build this feature view before getting output fields."
            )

        key_fields = [
            key
            for feature in self.get_resolved_features()
            if feature.keys is not None
            for key in feature.keys
        ]
        output_fields = [f for f in source_fields if f in key_fields]
        output_fields.append(self.timestamp_field)
        output_fields.extend([feature.name for feature in self.get_resolved_features()])

        return list(OrderedDict.fromkeys(output_fields))

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
                feature_descriptors=[self.source],
                force_update=force_update,
                props=props,
            )[0]

        features = []
        for feature in self.features:
            if isinstance(feature, str):
                source_feature = source.get_feature(feature)
                feature = Feature(
                    name=source_feature.name,
                    dtype=source_feature.dtype,
                    transform=ExpressionTransform(source_feature.name),
                    keys=source_feature.keys,
                )
            if feature.name == self.timestamp_field:
                continue
            features.append(feature)

        self._validate(source, features)
        props = {} if props is None else props
        feature_view = SlidingFeatureView(
            name=self.name,
            source=source,
            features=features,
            timestamp_field=self.timestamp_field,
            timestamp_format=self.timestamp_format,
            filter_expr=self.filter_expr,
            extra_props={**props, **self.config.original_props},
        )
        return feature_view

    @append_metadata_to_json
    def to_json(self) -> Dict:
        features: List[Union[str, Dict]] = []
        for feature in self.features:
            if isinstance(feature, str):
                features.append(feature)
            elif feature.name != self.timestamp_field:
                features.append(feature.to_json())
        return {
            "name": self.name,
            "source": (
                self.source if isinstance(self.source, str) else self.source.to_json()
            ),
            "features": features,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "filter_expr": self.filter_expr,
            "extra_props": self.config.original_props,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "SlidingFeatureView":
        return SlidingFeatureView(
            name=json_dict["name"],
            source=json_dict["source"]
            if isinstance(json_dict["source"], str)
            else from_json(json_dict["source"]),
            features=[
                feature if isinstance(feature, str) else from_json(feature)
                for feature in json_dict["features"]
            ],
            timestamp_field=json_dict["timestamp_field"],
            timestamp_format=json_dict["timestamp_format"],
            filter_expr=json_dict["filter_expr"],
            extra_props=json_dict["extra_props"],
        )

    def _validate(self, source: TableDescriptor, features: Sequence[Feature]) -> None:
        pre_sliding_features: Set[Feature] = set()
        sliding_features: Set[Feature] = set()
        post_sliding_features: Set[Feature] = set()

        for feature in features:
            transform = feature.transform
            if isinstance(transform, ExpressionTransform):
                if len(sliding_features) == 0:
                    pre_sliding_features.add(feature)
                else:
                    post_sliding_features.add(feature)
            elif isinstance(transform, SlidingWindowTransform):
                sliding_features.add(feature)
            elif isinstance(transform, PythonUdfTransform):
                if len(sliding_features) == 0:
                    pre_sliding_features.add(feature)
                else:
                    post_sliding_features.add(feature)
            else:
                raise FeathubException(
                    f"Feature '{feature.name}' uses unsupported transform type "
                    f"'{type(transform)}'."
                )

        self._validate_pre_sliding_features(source, pre_sliding_features)
        self._validate_sliding_features(source, pre_sliding_features, sliding_features)
        self._validate_post_sliding_features(
            sliding_features, post_sliding_features, self.timestamp_field
        )

    @staticmethod
    def _validate_pre_sliding_features(
        source: TableDescriptor, pre_sliding_features: Set[Feature]
    ) -> None:
        valid_variables = set([f.name for f in source.get_output_features()])

        # Check if the pre sliding feature only depends on the features specified
        # earlier or the features in the source table.
        for feature in pre_sliding_features:
            transform = feature.transform
            if isinstance(transform, ExpressionTransform):
                variables = get_variables(transform.expr)
            elif isinstance(transform, PythonUdfTransform):
                variables = {f.name for f in feature.input_features}
            else:
                raise FeathubException(
                    f"Pre sliding feature '{feature.name}' uses unsupported transform "
                    f"type '{type(transform)}'."
                )

            if not variables.issubset(valid_variables):
                raise FeathubException(
                    f"Feature {feature} should only depend on features specified "
                    f"earlier or the features in the source table."
                )
            valid_variables.add(feature.name)

    @staticmethod
    def _validate_sliding_features(
        source: TableDescriptor,
        pre_sliding_features: Set[Feature],
        sliding_features: Set[Feature],
    ) -> None:
        if len(sliding_features) < 1:
            raise FeathubException(
                "SlidingWindowFeatureView must have at least one feature with "
                "SlidingWindowTransform."
            )

        valid_variables = {
            *[f.name for f in source.get_output_features()],
            *[f.name for f in pre_sliding_features],
        }

        # Check if the sliding feature only depends on the pre sliding features and
        # the features in the source table.
        sliding_window_transforms = []
        for feature in sliding_features:
            transform = feature.transform
            if isinstance(transform, SlidingWindowTransform):
                variables = set.union(
                    get_variables(transform.filter_expr)
                    if transform.filter_expr is not None
                    else set(),
                    get_variables(transform.expr),
                    transform.group_by_keys,
                )
                sliding_window_transforms.append(transform)
            else:
                raise FeathubException(
                    f"Sliding feature '{feature.name}' uses unsupported transform "
                    f"type '{type(transform)}'."
                )

            if not variables.issubset(valid_variables):
                raise FeathubException(
                    f"Feature {feature} should only depend on features specified "
                    f"earlier or the features in the source table."
                )

        group_by_keys_set = set(
            [tuple(transform.group_by_keys) for transform in sliding_window_transforms]
        )
        if len(group_by_keys_set) > 1:
            raise FeathubException(
                f"SlidingWindowTransforms have different group-by keys "
                f"{group_by_keys_set}."
            )

        steps = set([transform.step_size for transform in sliding_window_transforms])
        if len(steps) > 1:
            raise FeathubException(
                f"SlidingWindowTransforms have different step size " f"{steps}."
            )

        # Validate that the per-row transform features are used group-by-keys of
        # SlidingWindowTransform.
        invalid_expression_feature_names = set()
        group_by_keys = group_by_keys_set.pop()
        for feature in pre_sliding_features:
            if feature.name not in group_by_keys:
                invalid_expression_feature_names.add(feature.name)
        if len(invalid_expression_feature_names) != 0:
            raise FeathubException(
                f"{invalid_expression_feature_names} are not used as grouping key of "
                f"the sliding windows."
            )

    @staticmethod
    def _validate_post_sliding_features(
        sliding_features: Set[Feature],
        post_sliding_features: Set[Feature],
        timestamp_field: str,
    ) -> None:
        sliding_window_group_by_keys = cast(
            SlidingWindowTransform, next(iter(sliding_features)).transform
        ).group_by_keys
        valid_variables = {
            *[f.name for f in sliding_features],
            timestamp_field,
            *sliding_window_group_by_keys,
        }
        # Check if the post sliding feature only depends on the timestamp field,
        # sliding window features and group-by keys.
        for feature in post_sliding_features:
            transform = feature.transform
            if isinstance(transform, ExpressionTransform):
                variables = get_variables(transform.expr)
            elif isinstance(transform, PythonUdfTransform):
                variables = {f.name for f in feature.input_features}
            else:
                raise FeathubException(
                    f"Unexpected transformation: {transform} after sliding window."
                )

            if not variables.issubset(valid_variables):
                raise FeathubException(
                    f"Feature {feature} after sliding window should "
                    f"only depend on timestamp field, sliding window features, or "
                    f"group-by keys."
                )
            valid_variables.add(feature.name)

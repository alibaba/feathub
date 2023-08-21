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
from typing import Union, Optional, Dict, Sequence, Collection
import json

from feathub.common.exceptions import FeathubException
from feathub.common.types import DType
from feathub.common.utils import append_metadata_to_json, from_json
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.feature_views.transforms.transformation import Transformation
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.metric_stores.metric import Metric


def get_default_feature_name(disallowed_names: Collection[str]) -> str:
    """
    Returns a default name for a feature. Values in the disallowed_names would
    not be returned.
    """
    index = 0
    while f"_{index}" in disallowed_names:
        index += 1
    return f"_{index}"


class Feature:
    """
    A feature belongs to a table (e.g. Source, FeatureView). It is uniquely
    identified by the table name and feature name in a registry.
    """

    def __init__(
        self,
        name: str,
        transform: Union[str, Transformation],
        dtype: Optional[DType] = None,
        keys: Optional[Sequence[str]] = None,
        input_features: Sequence[Feature] = (),
        description: str = "",
        extra_props: Optional[Dict[str, str]] = None,
        metrics: Optional[Sequence[Metric]] = None,
    ):
        """
        :param name: The name that uniquely identifies this feature in the
                     parent table. Must not start or end with double underscores(__)
                     in order to avoid potential conflict with metadata columns.
        :param dtype: The data type of this feature's values.
        :param transform: The logic used to derive this feature's values. If it is a
                          string, it represents a FeatHub expression.
        :param keys: Optional. The names of fields in the parent table that are
                     necessary to interpret this feature's values. These fields
                     should be included in the join keys when joining this feature onto
                     another table. If it is None, its value will be derived from either
                     the `transform` or the parent table's keys.
        :param input_features: The names of fields in the parent table used by
                              `transform` to derive this feature's values.
        :param description: The description of the feature.
        :param extra_props: The extra properties of the feature that are defined by
                            user.
        :param metrics: The metrics of this feature.
        """
        if name.startswith("__") or name.endswith("__"):
            raise FeathubException(
                f"Feature name {name} should not start or end with double "
                f"underscores(__)."
            )
        self.name = name
        self.dtype = dtype
        if isinstance(transform, str):
            transform = ExpressionTransform(transform)
        self.transform = transform

        # If feature's keys are not specified, use group-by keys as the feature's keys.
        # Otherwise, validate that feature's keys contain group-by-keys.
        if isinstance(transform, OverWindowTransform) or isinstance(
            transform, SlidingWindowTransform
        ):
            if keys is None:
                keys = list(transform.group_by_keys)
            if not set(transform.group_by_keys).issubset(set(keys)):
                raise RuntimeError(
                    f"Feature keys {keys} should contain {transform.group_by_keys}."
                )
        if keys:
            keys = sorted(keys)
        self.keys = keys
        self.input_features = input_features
        self.description = description
        self.extra_props = {} if extra_props is None else extra_props
        self.metrics = [] if metrics is None else metrics

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "dtype": None if self.dtype is None else self.dtype.to_json(),
            "transform": self.transform.to_json(),
            "keys": self.keys,
            "input_features": [feature.to_json() for feature in self.input_features],
            "description": self.description,
            "extra_props": self.extra_props,
            "metrics": [metric.to_json() for metric in self.metrics],
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "Feature":
        return Feature(
            name=json_dict["name"],
            dtype=from_json(json_dict["dtype"])
            if json_dict["dtype"] is not None
            else None,
            transform=from_json(json_dict["transform"]),
            keys=json_dict["keys"],
            input_features=[
                from_json(feature) for feature in json_dict["input_features"]
            ],
            description=json_dict["description"],
            extra_props=json_dict["extra_props"],
            metrics=[from_json(metric) for metric in json_dict["metrics"]],
        )

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash(((k, v) for k, v in self.to_json().items()))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Feature) and self.to_json() == other.to_json()

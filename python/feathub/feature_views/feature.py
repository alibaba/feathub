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
from typing import Union, Optional, Dict, Sequence
import json

from feathub.common.types import DType
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.feature_views.transforms.transformation import Transformation
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.expression_transform import ExpressionTransform


# TODO: enforce dtype
class Feature:
    """
    A feature belongs to a table (e.g. Source, FeatureView). It is uniquely
    identified by the table name and feature name in a registry.
    """

    def __init__(
        self,
        name: str,
        dtype: DType,
        transform: Union[str, Transformation],
        keys: Optional[Sequence[str]] = None,
        input_features: Sequence[Feature] = (),
    ):
        """
        :param name: The name that uniquely identifies this feature in the
                     parent table.
        :param dtype: The data type of this feature's values.
        :param transform: The logic used to derive this feature's values. If it is a
                          string, it represents a Feathub expression.
        :param keys: Optional. The names of fields in the parent table that are
                     necessary to interpret this feature's values. These fields
                     should be included in the join keys when joining this feature onto
                     another table. If it is None, its value will be derived from either
                     the `transform` or the parent table's keys.
        :param input_features: The names of fields in the parent table used by
                              `transform` to derive this feature's values.
        """
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

    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "dtype": self.dtype.to_json(),
            "transform": self.transform.to_json(),
            "keys": self.keys,
            "input_features": [feature.to_json() for feature in self.input_features],
        }

    # TODO: add from_json()

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Feature) and self.to_json() == other.to_json()

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
import collections
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional, Dict, OrderedDict, Sequence, Callable

from feathub.common.exceptions import FeathubException
from feathub.common.utils import append_metadata_to_json
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.feature_views.transforms.transformation import Transformation


# TODO: Add document to record built-in metrics.
class Metric(ABC):
    """
    Common interface for all metrics. A metric refers to a statistic of a
    characteristic of a feature.
    """

    def __init__(self, metric_type: str, window_size: timedelta = timedelta(seconds=0)):
        """
        :param metric_type: The type of the metric.
        :param window_size: The time range to compute the metric. It should be zero or
                            a positive time span. If it is zero, the metric will be
                            computed from all features that have been processed since
                            the Feathub job is created.
        """
        if window_size <= timedelta(seconds=0):
            raise FeathubException(
                f"Metric window size {window_size} must be a positive value."
            )

        self.metric_type = metric_type
        self.window_size = window_size

    def get_tags(self) -> OrderedDict:
        """
        Gets the tags of this metric for tag-based metric reporters.
        """
        return collections.OrderedDict(
            [("window_time_sec", str(int(self.window_size / timedelta(seconds=1))))]
        )

    @abstractmethod
    def get_transform_functions(self) -> Sequence[Callable[[str], Transformation]]:
        """
        Gets the functions to generate transformations used to aggregate and compute the
        metric value. The function has the following parameter:

        - feature_name: Name of a variable containing the intermediate metric value
                        generated from previously defined transformation. For the first
                        transformation, its value is the host feature name.
        """
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        """
        Returns a json-formatted object representing this metric.
        """
        pass


class Count(Metric):
    """
    A Metric that shows the number of features.
    """

    def __init__(
        self,
        filter_expr: Optional[str] = None,
        window_size: timedelta = timedelta(seconds=0),
    ):
        """
        :param filter_expr: Optional. If it is not None, it represents a partial FeatHub
                            expression which evaluates to a boolean value. The partial
                            Feathub expression should be a binary operator whose left
                            child is absent and would be filled in with the host feature
                            name. For example, "IS NULL" will be enriched into
                            "{feature_name} IS NULL". Only features that evaluate this
                            expression into True will be considered when computing the
                            metric.
        :param window_size: The time range to compute the metric. It should be zero or
                            a positive time span. If it is zero, the metric will be
                            computed from all features that have been processed since
                            the Feathub job is created.
        """
        super().__init__("count", window_size)
        self.filter_expr = filter_expr

    def get_tags(self) -> OrderedDict:
        tags = super(Count, self).get_tags()
        tags["filter_expr"] = "" if self.filter_expr is None else self.filter_expr
        return tags

    def get_transform_functions(self) -> Sequence[Callable[[str], Transformation]]:
        return [
            lambda feature_name: SlidingWindowTransform(
                expr="1",
                agg_func="COUNT",
                window_size=self.window_size,
                step_size=self.window_size,
                filter_expr=None
                if self.filter_expr is None
                else f"{feature_name} {self.filter_expr}",
            )
        ]

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "window_size_sec": self.window_size / timedelta(seconds=1),
            "filter_expr": self.filter_expr,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "Count":
        return Count(
            filter_expr=json_dict["filter_expr"],
            window_size=timedelta(seconds=json_dict["window_size_sec"]),
        )


class Ratio(Metric):
    """
    A Metric that shows the proportion of the number features that meets filter_expr
    to the number of all features.
    """

    def __init__(
        self,
        filter_expr: str,
        window_size: timedelta = timedelta(seconds=0),
    ):
        """
        :param filter_expr: A partial FeatHub expression which evaluates to a boolean
                            value. The partial Feathub expression should be a binary
                            operator whose left child is absent and would be filled in
                            with the host feature name. For example, "IS NULL" will be
                            enriched into "{feature_name} IS NULL". Only features that
                            evaluate this expression into True will be considered when
                            computing the metric.
        :param window_size: The time range to compute the metric. It should be zero or
                            a positive time span. If it is zero, the metric will be
                            computed from all features that have been processed since
                            the Feathub job is created.
        """
        super().__init__("ratio", window_size)
        self.filter_expr = filter_expr

    def get_tags(self) -> OrderedDict:
        tags = super(Ratio, self).get_tags()
        tags["filter_expr"] = self.filter_expr
        return tags

    def get_transform_functions(self) -> Sequence[Callable[[str], Transformation]]:
        return [
            lambda feature_name: SlidingWindowTransform(
                expr=f"CASE WHEN {feature_name} {self.filter_expr} "
                f"THEN 1 ELSE 0 END",
                agg_func="AVG",
                window_size=self.window_size,
                step_size=self.window_size,
            ),
            lambda feature_name: ExpressionTransform(
                f"CASE WHEN {feature_name} IS NULL THEN 0 ELSE " f"{feature_name} END",
            ),
        ]

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "window_size_sec": self.window_size / timedelta(seconds=1),
            "filter_expr": self.filter_expr,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "Ratio":
        return Ratio(
            filter_expr=json_dict["filter_expr"],
            window_size=timedelta(seconds=json_dict["window_size_sec"]),
        )

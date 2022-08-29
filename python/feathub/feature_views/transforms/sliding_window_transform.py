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
from datetime import timedelta
from typing import Union, Sequence, Optional, Dict

from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.transformation import Transformation


class SlidingWindowTransform(Transformation):
    """
    Derives feature values by applying Feathub expression and aggregation function on
    multiple rows in a sliding window.
    """

    def __init__(
        self,
        expr: str,
        agg_func: Union[str, AggFunc],
        window_size: timedelta,
        step_size: timedelta,
        group_by_keys: Sequence[str] = (),
        filter_expr: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        """
        :param expr: A Feathub expression composed of UDF and feature names.
        :param agg_func: The aggregation function or the name of the aggregation
                         function as string such as "MAX", "AVG".
        :param window_size: The size of the sliding window.
        :param step_size: The step_size specifies how often the sliding windows starts.
        :param group_by_keys: The names of fields to be used as the grouping key.
        :param filter_expr: Optional. If it is not None, it represents a Feathub
                            expression. Only rows that match the filter expression can
                            be included in a sliding window.
        :param limit: Optional. If it is not None, up to `limit` number of most recent
                      rows that match the `filter_expr` in a sliding window will be
                      aggregated.
        """
        super().__init__()
        self.expr = expr
        self.agg_func = agg_func if isinstance(agg_func, AggFunc) else AggFunc(agg_func)
        self.window_size = window_size
        self.step_size = step_size
        self.group_by_keys = group_by_keys
        self.filter_expr = filter_expr
        self.limit = limit

    def to_json(self) -> Dict:
        return {
            "type": "SlidingWindowTransform",
            "expr": self.expr,
            "agg_func": self.agg_func.value,
            "group_by_keys": self.group_by_keys,
            "window_size_ms": self.window_size / timedelta(milliseconds=1),
            "step_ms": self.step_size / timedelta(milliseconds=1),
            "filter_expr": self.filter_expr,
            "limit": self.limit,
        }

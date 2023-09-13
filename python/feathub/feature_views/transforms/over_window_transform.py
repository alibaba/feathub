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
from typing import Optional, Dict, Sequence, Union
from datetime import timedelta

from feathub.common.utils import append_metadata_to_json
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.transformation import Transformation


class OverWindowTransform(Transformation):
    """
    Derives feature values by applying FeatHub expression and aggregation function on
    multiple rows of the parent table at a time.
    """

    def __init__(
        self,
        expr: str,
        agg_func: Union[str, AggFunc],
        window_size: Optional[timedelta] = None,
        group_by_keys: Sequence[str] = (),
        filter_expr: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        """
        :param expr: A FeatHub expression composed of UDF and feature names.
        :param agg_func: The aggregation function or the name of the aggregation
                         function as string such as "MAX", "AVG".
        :param window_size: Optional. If it is not None, for any row in the table with
                            timestamp = t0, only rows whose timestamp fall in range
                            [t0 - timedelta, t0] and meet the `filter_expr` can be
                            included in the aggregation. If it is None, the window size
                            is effectively unlimited.
        :param group_by_keys: The names of fields to be used as the grouping key.
        :param filter_expr: Optional. If it is not None, it represents a FeatHub
                            expression. Only rows that match the filter expression can
                            be included in the over window.
        :param limit: Optional. If it is not None, up to `limit` number of most recent
                      rows that match the `filter_expr` prior to this row can be
                      included in the aggregation.
        """
        super().__init__()
        self.expr = expr
        self.agg_func = (
            agg_func if isinstance(agg_func, AggFunc) else AggFunc(agg_func.upper())
        )
        self.group_by_keys = group_by_keys
        self.window_size = window_size
        self.filter_expr = filter_expr
        self.limit = limit

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "expr": self.expr,
            "agg_func": self.agg_func.value,
            "group_by_keys": self.group_by_keys,
            "window_size_ms": None
            if self.window_size is None
            else self.window_size / timedelta(milliseconds=1),
            "filter_expr": self.filter_expr,
            "limit": self.limit,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "OverWindowTransform":
        return OverWindowTransform(
            expr=json_dict["expr"],
            agg_func=json_dict["agg_func"],
            group_by_keys=json_dict["group_by_keys"],
            window_size=timedelta(milliseconds=json_dict["window_size_ms"])
            if json_dict["window_size_ms"] is not None
            else None,
            filter_expr=json_dict["filter_expr"],
            limit=json_dict["limit"],
        )

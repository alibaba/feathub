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
from datetime import timedelta, datetime, tzinfo
from typing import Optional, Sequence, Type, Any, Tuple, Dict, List

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.common.types import to_numpy_dtype
from feathub.dsl.ast import ExprAST
from feathub.dsl.expr_parser import ExprParser
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.local.aggregation_utils import AGG_FUNCTIONS
from feathub.processors.local.ast_evaluator.local_ast_evaluator import LocalAstEvaluator
from feathub.processors.local.time_utils import append_unix_time_column
from feathub.processors.type_utils import cast_dataframe_dtype


# TODO: Unify common classes across all processors.
class AggregationFieldDescriptor:
    """
    Descriptor of a field computed by aggregation.
    """

    def __init__(
        self,
        field_name: str,
        field_data_type: Type,
        expr: str,
        agg_func: AggFunc,
        window_size: timedelta,
        filter_expr: Optional[str],
        limit: Optional[int],
    ) -> None:
        self.field_name = field_name
        self.field_data_type = field_data_type
        self.expr = expr
        self.agg_func = agg_func
        self.window_size = window_size
        self.filter_expr = filter_expr
        self.limit = limit

    @staticmethod
    def from_feature(feature: Feature) -> "AggregationFieldDescriptor":
        transform = feature.transform
        if not (isinstance(transform, SlidingWindowTransform)):
            raise FeathubException(
                f"Cannot convert {feature} to AggregationFieldDescriptor."
            )
        return AggregationFieldDescriptor(
            feature.name,
            to_numpy_dtype(feature.dtype),
            transform.expr,
            transform.agg_func,
            transform.window_size,
            transform.filter_expr,
            transform.limit,
        )


class SlidingWindowDescriptor:
    """
    Descriptor of a sliding window.
    """

    def __init__(
        self,
        step_size: timedelta,
        group_by_keys: Sequence[str],
    ) -> None:
        self.step_size = step_size
        self.group_by_keys = group_by_keys

    @staticmethod
    def from_sliding_window_transform(
        sliding_window_agg: SlidingWindowTransform,
    ) -> "SlidingWindowDescriptor":
        return SlidingWindowDescriptor(
            sliding_window_agg.step_size,
            sliding_window_agg.group_by_keys,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.step_size == other.step_size
            and self.group_by_keys == other.group_by_keys
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.step_size,
                tuple(self.group_by_keys),
            )
        )


def evaluate_sliding_window(
    input_df: pd.DataFrame,
    feature_view: SlidingFeatureView,
    window_descriptor: SlidingWindowDescriptor,
    agg_descriptors: List[AggregationFieldDescriptor],
    tz: tzinfo,
    parser: ExprParser,
    ast_evaluator: LocalAstEvaluator,
) -> pd.DataFrame:
    """
    Evaluate the sliding window on the input DataFrame.
    """
    df_copy = input_df.copy()
    append_unix_time_column(
        df_copy,
        feature_view.get_resolved_source().timestamp_field,
        feature_view.get_resolved_source().timestamp_format,
        tz,
    )

    agg_df = df_copy.sort_values(by=[EVENT_TIME_ATTRIBUTE_NAME])
    if len(window_descriptor.group_by_keys) > 0:
        agg_df = agg_df.groupby(by=window_descriptor.group_by_keys).apply(
            lambda df: _sliding_window_func(
                df=df,
                sliding_window_descriptor=window_descriptor,
                agg_field_descriptors=agg_descriptors,
                tz=tz,
                parser=parser,
                ast_evaluator=ast_evaluator,
            )
        )
    else:
        agg_df = _sliding_window_func(
            df=agg_df,
            sliding_window_descriptor=window_descriptor,
            agg_field_descriptors=agg_descriptors,
            tz=tz,
            parser=parser,
            ast_evaluator=ast_evaluator,
        )

    agg_df = agg_df.reset_index(drop=True)

    # Compute the timestamp field with the given timestamp format from event
    # time(window time).
    if feature_view.timestamp_field is not None:
        if feature_view.timestamp_format == "epoch":
            agg_df[feature_view.timestamp_field] = agg_df[
                EVENT_TIME_ATTRIBUTE_NAME
            ].transform(lambda unix_time: int(unix_time))
        elif feature_view.timestamp_format == "epoch_millis":
            agg_df[feature_view.timestamp_field] = agg_df[
                EVENT_TIME_ATTRIBUTE_NAME
            ].transform(lambda unix_time: int(unix_time * 1000))
        else:
            agg_df[feature_view.timestamp_field] = agg_df[
                EVENT_TIME_ATTRIBUTE_NAME
            ].transform(
                lambda unix_time: datetime.fromtimestamp(unix_time).strftime(
                    feature_view.timestamp_format
                )[:-3]
            )

    agg_df = agg_df.drop([EVENT_TIME_ATTRIBUTE_NAME], axis=1)
    return agg_df


def _sliding_window_func(
    df: pd.DataFrame,
    sliding_window_descriptor: SlidingWindowDescriptor,
    agg_field_descriptors: Sequence[AggregationFieldDescriptor],
    tz: tzinfo,
    parser: ExprParser,
    ast_evaluator: LocalAstEvaluator,
) -> Optional[pd.DataFrame]:

    if df.shape[0] <= 0:
        return None

    df_copy = df.copy()

    # We assign row base on the local timestamp millis instead of unix time so that the
    # windows are aligned with 1970-01-01 00:00:00 at the current time zone.
    df_copy[EVENT_TIME_ATTRIBUTE_NAME] = df_copy[EVENT_TIME_ATTRIBUTE_NAME].transform(
        lambda t: int((t + _get_utc_offset_seconds(t, tz)) * 1000)
    )

    first_row = df_copy.iloc[0]
    keys_dict = {k: first_row[k] for k in sliding_window_descriptor.group_by_keys}
    agg_field_names = [d.field_name for d in agg_field_descriptors]

    filter_ast_map: Dict[AggregationFieldDescriptor, ExprAST] = {
        descriptor: parser.parse(descriptor.filter_expr)
        for descriptor in agg_field_descriptors
        if descriptor.filter_expr is not None
    }
    agg_result_dtypes = {
        agg_field_descriptor.field_name: agg_field_descriptor.field_data_type
        for agg_field_descriptor in agg_field_descriptors
    }
    step_size_millis = int(sliding_window_descriptor.step_size.total_seconds() * 1000)

    for agg_field_descriptor in agg_field_descriptors:
        expr_node = parser.parse(agg_field_descriptor.expr)
        df_copy[agg_field_descriptor.field_name] = df_copy.apply(
            lambda r: ast_evaluator.eval(expr_node, r), axis=1
        )

    res_df = pd.DataFrame()
    idx_map: Dict[AggregationFieldDescriptor, Tuple[int, int]] = {}
    cur_window_end = _get_first_window_end_time(
        first_row[EVENT_TIME_ATTRIBUTE_NAME], sliding_window_descriptor.step_size
    )

    while True:
        agg_res: Dict[str, Any] = {}
        all_reach_end = True
        for agg_field_descriptor in agg_field_descriptors:
            agg_func = AGG_FUNCTIONS.get(agg_field_descriptor.agg_func, None)
            if agg_func is None:
                raise RuntimeError(
                    f"Unsupported agg function {agg_field_descriptor.agg_func}."
                )

            left_idx, right_idx = idx_map.get(agg_field_descriptor, (0, 0))

            # Advance left idx
            while (
                right_idx < df_copy.shape[0]
                and df_copy.iloc[right_idx][EVENT_TIME_ATTRIBUTE_NAME] < cur_window_end
            ):
                right_idx += 1

            # Advance right idx
            window_size_millis = int(
                agg_field_descriptor.window_size.total_seconds() * 1000
            )
            while (
                left_idx < right_idx
                and df_copy.iloc[left_idx][EVENT_TIME_ATTRIBUTE_NAME]
                < cur_window_end - window_size_millis
            ):
                left_idx += 1

            if left_idx >= df_copy.shape[0] and right_idx >= df_copy.shape[0]:
                # All rows are processed for this aggregation descriptor
                rows_in_window = df_copy.iloc[0:0]
            else:
                all_reach_end = False
                rows_in_window = df_copy.iloc[left_idx:right_idx]

            if agg_field_descriptor in filter_ast_map and rows_in_window.shape[0] > 0:
                # Filter the rows in the window
                rows_in_window = rows_in_window[
                    rows_in_window.apply(
                        lambda r: ast_evaluator.eval(
                            filter_ast_map.get(agg_field_descriptor), r
                        ),
                        axis=1,
                    )
                ]

            limit = agg_field_descriptor.limit
            if limit is not None and limit < rows_in_window.shape[0]:
                # Limit the rows in the window
                rows_in_window = rows_in_window.iloc[-limit:]
            agg_res[agg_field_descriptor.field_name] = agg_func(
                rows_in_window[agg_field_descriptor.field_name].to_list()
            )

            # Update indexes
            idx_map[agg_field_descriptor] = (left_idx, right_idx)

        last_row_agg_res = None
        if res_df.shape[0] > 0:
            last_row_agg_res = dict(res_df.iloc[-1][agg_field_names])

        if agg_res != last_row_agg_res:
            row = pd.DataFrame(
                data={
                    **keys_dict,
                    **{agg_field: [agg_val] for agg_field, agg_val in agg_res.items()},
                    EVENT_TIME_ATTRIBUTE_NAME: [cur_window_end - 1],
                }
            )
            res_df = res_df.append(cast_dataframe_dtype(row, agg_result_dtypes))

        if all_reach_end:
            break

        cur_window_end += step_size_millis

    # Convert local timestamp mills back to unix time
    res_df[EVENT_TIME_ATTRIBUTE_NAME] = res_df[EVENT_TIME_ATTRIBUTE_NAME].transform(
        lambda t: t / 1000.0 - _get_utc_offset_seconds(t / 1000.0, tz)
    )
    res_df.index = res_df[EVENT_TIME_ATTRIBUTE_NAME]
    return res_df


def _get_utc_offset_seconds(unix_time: float, zone: tzinfo) -> float:
    return zone.utcoffset(datetime.fromtimestamp(unix_time)).total_seconds()


def _get_first_window_end_time(
    row_time: int,
    step_size: timedelta,
) -> int:
    step_size_millis = int(step_size.total_seconds() * 1000)
    first_window_end = row_time - row_time % step_size_millis + step_size_millis
    return first_window_end

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
from typing import List, Optional, Sequence, Any, Dict, Callable

import pandas as pd
from pyflink.table import (
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    AggregateFunction,
)
from pyflink.table.types import DataType
from pyflink.table.udf import udaf, ACC, T
from pyflink.table.window import OverWindowPartitionedOrderedPreceding, Over

from feathub.common.exceptions import FeathubTransformationException
from feathub.feature_views.transforms.agg_func import AggFunc

from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)


def _avg(s: pd.Series) -> Any:
    return s.mean()


def _min(s: pd.Series) -> Any:
    return s.min()


def _max(s: pd.Series) -> Any:
    return s.max()


def _sum(s: pd.Series) -> Any:
    return s.sum()


def _first_value(s: pd.Series) -> Any:
    return s.iloc[0]


def _last_value(s: pd.Series) -> Any:
    return s.iloc[-1]


def _row_num(s: pd.Series) -> Any:
    return s.size


_AGG_FUNCTIONS: Dict[AggFunc, Callable[[pd.Series], Any]] = {
    AggFunc.AVG: _avg,
    AggFunc.SUM: _sum,
    AggFunc.MAX: _max,
    AggFunc.MIN: _min,
    AggFunc.FIRST_VALUE: _first_value,
    AggFunc.LAST_VALUE: _last_value,
    AggFunc.ROW_NUMBER: _row_num,
}


class OverWindowDescriptor:
    """
    Descriptor of an over window.
    """

    def __init__(
        self,
        window_size: Optional[timedelta],
        limit: Optional[int],
        group_by_keys: Sequence[str],
        filter_expr: Optional[str],
    ) -> None:
        self.window_size = window_size
        self.limit = limit
        self.group_by_keys = group_by_keys
        self.filter_expr = filter_expr

    @staticmethod
    def from_over_window_transform(
        window_agg_transform: OverWindowTransform,
    ) -> "OverWindowDescriptor":
        return OverWindowDescriptor(
            window_agg_transform.window_size,
            window_agg_transform.limit,
            window_agg_transform.group_by_keys,
            window_agg_transform.filter_expr,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.window_size == other.window_size
            and self.limit == other.limit
            and self.group_by_keys == other.group_by_keys
            and self.filter_expr == other.filter_expr
        )

    def __hash__(self) -> int:
        return hash(
            (self.window_size, self.limit, tuple(self.group_by_keys), self.filter_expr)
        )


def evaluate_over_window_transform(
    flink_table: NativeFlinkTable,
    window_descriptor: "OverWindowDescriptor",
    agg_descriptors: List["AggregationFieldDescriptor"],
    time_attribute: str,
) -> NativeFlinkTable:
    """
    Evaluate the over window transforms on the given flink table and return the
    result table.

    :param flink_table: The input Flink table.
    :param window_descriptor: The descriptor of the over window.
    :param agg_descriptors: A list of descriptor that descriptor the aggregation to
                            perform.
    :param time_attribute: The field name of the time attribute of the `flink_table`.
    :return:
    """
    window = _get_flink_over_window(window_descriptor, time_attribute)
    if window_descriptor.filter_expr is not None:
        agg_table = (
            flink_table.filter(
                native_flink_expr.call_sql(window_descriptor.filter_expr)
            )
            .over_window(window.alias("w"))
            .select(
                native_flink_expr.col("*"),
                *_get_over_window_agg_column_list(
                    window_descriptor, agg_descriptors, time_attribute
                ),
            )
        )

        # For rows that do not satisfy the filter predicate, set the feature col
        # to NULL.
        null_feature_table = flink_table.filter(
            native_flink_expr.not_(
                native_flink_expr.call_sql(window_descriptor.filter_expr)
            )
        ).add_columns(
            *[
                native_flink_expr.null_of(descriptor.field_data_type).alias(
                    descriptor.field_name
                )
                for descriptor in agg_descriptors
            ]
        )

        # After union, order of the row with same grouping key is not preserved. We
        # can only preserve the order of the row with the same grouping keys and
        # filter condition.
        return agg_table.union_all(null_feature_table)

    return flink_table.over_window(window.alias("w")).select(
        native_flink_expr.col("*"),
        *_get_over_window_agg_column_list(
            window_descriptor, agg_descriptors, time_attribute
        ),
    )


def _get_flink_over_window(
    over_window_descriptor: "OverWindowDescriptor",
    time_attribute: str,
) -> OverWindowPartitionedOrderedPreceding:

    # Group by key
    if len(over_window_descriptor.group_by_keys) == 0:
        window = Over.order_by(native_flink_expr.col(time_attribute))
    else:
        keys = [
            native_flink_expr.col(key) for key in over_window_descriptor.group_by_keys
        ]
        window = Over.partition_by(*keys).order_by(
            native_flink_expr.col(time_attribute)
        )

    if over_window_descriptor.limit is not None:
        # Flink over window only support ranging by either row-count or time. For
        # feature that need to range by both row-count and time, it is handled in
        # _get_over_window_agg_select_expr with the _TimeWindowedAggFunction UDTAF.
        return window.preceding(
            native_flink_expr.row_interval(over_window_descriptor.limit - 1)
        )

    if over_window_descriptor.window_size is not None:
        return window.preceding(
            native_flink_expr.lit(
                over_window_descriptor.window_size / timedelta(milliseconds=1)
            ).milli
        )

    return window.preceding(native_flink_expr.UNBOUNDED_RANGE)


def _get_over_window_agg_column_list(
    window_descriptor: "OverWindowDescriptor",
    agg_descriptors: List["AggregationFieldDescriptor"],
    time_attribute: str,
) -> List[native_flink_expr.Expression]:
    return [
        _get_over_window_agg_select_expr(
            native_flink_expr.call_sql(descriptor.expr).cast(
                descriptor.field_data_type
            ),
            descriptor.field_data_type,
            window_descriptor,
            descriptor.agg_func,
            "w",
            time_attribute,
        ).alias(descriptor.field_name)
        for descriptor in agg_descriptors
    ]


def _get_over_window_agg_select_expr(
    expr: native_flink_expr.Expression,
    result_type: DataType,
    over_window_descriptor: "OverWindowDescriptor",
    agg_func: AggFunc,
    window_alias: str,
    time_attribute: str,
) -> native_flink_expr.Expression:

    if (
        over_window_descriptor.limit is not None
        and over_window_descriptor.window_size is not None
    ):
        # We use PyFlink UDAF to support over window ranged by both time interval
        # and row count.
        if agg_func not in _AGG_FUNCTIONS:
            raise FeathubTransformationException(
                f"Unsupported aggregation for FlinkProcessor {agg_func}."
            )
        time_windowed_agg_func = _TimeWindowedAggFunction(
            over_window_descriptor.window_size,
            _AGG_FUNCTIONS[agg_func],
        )
        result = native_flink_expr.call(
            udaf(time_windowed_agg_func, result_type=result_type, func_type="pandas"),
            expr,
            native_flink_expr.col(time_attribute),
        )
    else:
        if agg_func == AggFunc.AVG:
            result = expr.avg
        elif agg_func == AggFunc.MIN:
            result = expr.min
        elif agg_func == AggFunc.MAX:
            result = expr.max
        elif agg_func == AggFunc.SUM:
            result = expr.sum
        # TODO: FIRST_VALUE AND LAST_VALUE is supported after PyFlink 1.16 without
        # PyFlink UDAF.
        else:
            raise FeathubTransformationException(
                f"Unsupported aggregation for FlinkProcessor {agg_func}."
            )
    return result.over(native_flink_expr.col(window_alias))


# TODO: We can implement the TimeWindowedAggFunction in Java to get better performance.
class _TimeWindowedAggFunction(AggregateFunction):
    """
    An aggregate function for Flink table aggregation.

    The aggregate function only aggregate rows with row time in the range of
    [current_row_time - time_interval, current_row_time]. Currently, Flink SQL/Table
    only support over window with either time range or row count-based range. This can
    be used with a row count-based over window to achieve over window with both time
    range and row count-based range.
    """

    def __init__(self, time_interval: timedelta, agg_func: Callable[[pd.Series], Any]):
        """
        Instantiate a _TimeWindowedAggFunction.

        :param time_interval: Only rows with row time in the range of [current_row_time
                              - time_interval, current_row_time] is included in the
                              aggregation function.
        :param agg_func: The name of an aggregation function such as MAX, AVG.
        """
        self.time_interval = time_interval
        self.agg_op = agg_func

    def get_value(self, accumulator: ACC) -> T:
        return accumulator[0]

    def create_accumulator(self) -> ACC:
        return []

    def accumulate(self, accumulator: ACC, *args: pd.Series) -> None:
        df = pd.DataFrame({"val": args[0], "time": args[1]})
        latest_time = df["time"].iloc[-1]
        df = df[df.time >= latest_time - self.time_interval]
        accumulator.append(self.agg_op(df["val"]))

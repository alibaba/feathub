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
from typing import List, Optional, Sequence

from pyflink.table import (
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    StreamTableEnvironment,
)
from pyflink.table.window import OverWindowPartitionedOrderedPreceding, Over

from feathub.common.exceptions import FeathubTransformationException
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)
from feathub.processors.flink.table_builder.flink_sql_expr_utils import (
    to_flink_sql_expr,
)
from feathub.processors.flink.table_builder.flink_table_builder_constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)
from feathub.processors.flink.table_builder.udf import (
    ROW_AND_TIME_BASED_OVER_WINDOW_JAVA_UDF,
)


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
        filter_expr = (
            to_flink_sql_expr(window_agg_transform.filter_expr)
            if window_agg_transform.filter_expr is not None
            else None
        )
        return OverWindowDescriptor(
            window_agg_transform.window_size,
            window_agg_transform.limit,
            window_agg_transform.group_by_keys,
            filter_expr,
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
    t_env: StreamTableEnvironment,
    flink_table: NativeFlinkTable,
    window_descriptor: "OverWindowDescriptor",
    agg_descriptors: List["AggregationFieldDescriptor"],
) -> NativeFlinkTable:
    """
    Evaluate the over window transforms on the given flink table and return the
    result table.

    :param t_env: The StreamTableEnvironment.
    :param flink_table: The input Flink table.
    :param window_descriptor: The descriptor of the over window.
    :param agg_descriptors: A list of descriptor that descriptor the aggregation to
                            perform.
    :return:
    """
    window = _get_flink_over_window(window_descriptor)
    if window_descriptor.filter_expr is not None:
        agg_table = (
            flink_table.filter(
                native_flink_expr.call_sql(window_descriptor.filter_expr)
            )
            .over_window(window.alias("w"))
            .select(
                native_flink_expr.col("*"),
                *_get_over_window_agg_column_list(window_descriptor, agg_descriptors),
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
        result_table = agg_table.union_all(null_feature_table)
    else:
        result_table = flink_table.over_window(window.alias("w")).select(
            native_flink_expr.col("*"),
            *_get_over_window_agg_column_list(window_descriptor, agg_descriptors),
        )

    return result_table


def _get_flink_over_window(
    over_window_descriptor: "OverWindowDescriptor",
) -> OverWindowPartitionedOrderedPreceding:

    # Group by key
    if len(over_window_descriptor.group_by_keys) == 0:
        window = Over.order_by(native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME))
    else:
        keys = [
            native_flink_expr.col(key) for key in over_window_descriptor.group_by_keys
        ]
        window = Over.partition_by(*keys).order_by(
            native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME)
        )

    if over_window_descriptor.limit is not None:
        # Flink over window only support ranging by either row-count or time. For
        # feature that need to range by both row-count and time, it is handled in
        # _get_over_window_agg_select_expr with the TimeWindowedAggFunction UDTAF.
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
) -> List[native_flink_expr.Expression]:
    return [
        _get_over_window_agg_select_expr(
            native_flink_expr.call_sql(descriptor.expr),
            window_descriptor,
            descriptor.agg_func,
            "w",
        )
        .cast(descriptor.field_data_type)
        .alias(descriptor.field_name)
        for descriptor in agg_descriptors
    ]


def _get_over_window_agg_select_expr(
    expr: native_flink_expr.Expression,
    over_window_descriptor: "OverWindowDescriptor",
    agg_func: AggFunc,
    window_alias: str,
) -> native_flink_expr.Expression:

    if (
        over_window_descriptor.limit is not None
        and over_window_descriptor.window_size is not None
    ):
        interval_expr = native_flink_expr.call_sql(
            timedelta_to_flink_sql_interval(
                over_window_descriptor.window_size, day_precision=3
            )
        )

        java_udf_descriptor = ROW_AND_TIME_BASED_OVER_WINDOW_JAVA_UDF.get(
            agg_func, None
        )
        if java_udf_descriptor is None:
            raise FeathubTransformationException(
                f"Unsupported aggregation for FlinkProcessor {agg_func}."
            )

        return native_flink_expr.call(
            java_udf_descriptor.udf_name,
            interval_expr,
            expr,
            native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME),
        ).over(native_flink_expr.col(window_alias))

    if agg_func == AggFunc.AVG:
        result = expr.avg
    elif agg_func == AggFunc.MIN:
        result = expr.min
    elif agg_func == AggFunc.MAX:
        result = expr.max
    elif agg_func == AggFunc.SUM:
        result = expr.sum
    elif agg_func == AggFunc.VALUE_COUNTS:
        result = native_flink_expr.call("value_counts", expr)
    # TODO: FIRST_VALUE AND LAST_VALUE is supported after PyFlink 1.16 without
    # PyFlink UDAF.
    else:
        raise FeathubTransformationException(
            f"Unsupported aggregation for FlinkProcessor {agg_func}."
        )

    return result.over(native_flink_expr.col(window_alias))

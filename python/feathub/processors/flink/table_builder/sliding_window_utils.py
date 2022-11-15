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
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
)

from feathub.common.exceptions import FeathubTransformationException
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)
from feathub.processors.flink.table_builder.flink_sql_expr_utils import (
    to_flink_sql_expr,
)
from feathub.processors.flink.table_builder.flink_table_builder_constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.table_builder.join_utils import join_table_on_key
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)
from feathub.processors.flink.table_builder.udf import (
    AGG_JAVA_UDF,
)


class SlidingWindowDescriptor:
    """
    Descriptor of a sliding window.
    """

    def __init__(
        self,
        window_size: timedelta,
        step_size: timedelta,
        limit: Optional[int],
        group_by_keys: Sequence[str],
        filter_expr: Optional[str],
    ) -> None:
        self.window_size = window_size
        self.step_size = step_size
        self.limit = limit
        self.group_by_keys = group_by_keys
        self.filter_expr = filter_expr

    @staticmethod
    def from_sliding_window_transform(
        sliding_window_agg: SlidingWindowTransform,
    ) -> "SlidingWindowDescriptor":
        filter_expr = (
            to_flink_sql_expr(sliding_window_agg.filter_expr)
            if sliding_window_agg.filter_expr is not None
            else None
        )
        return SlidingWindowDescriptor(
            sliding_window_agg.window_size,
            sliding_window_agg.step_size,
            sliding_window_agg.limit,
            sliding_window_agg.group_by_keys,
            filter_expr,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.window_size == other.window_size
            and self.step_size == other.step_size
            and self.limit == other.limit
            and self.group_by_keys == other.group_by_keys
            and self.filter_expr == other.filter_expr
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.window_size,
                self.step_size,
                self.limit,
                tuple(self.group_by_keys),
                self.filter_expr,
            )
        )


def evaluate_sliding_window_transform(
    t_env: StreamTableEnvironment,
    flink_table: NativeFlinkTable,
    window_descriptor: SlidingWindowDescriptor,
    agg_descriptors: List["AggregationFieldDescriptor"],
) -> NativeFlinkTable:
    """
    Evaluate the sliding window transforms on the given flink table and return the
    result table.

    :param t_env: The StreamTableEnvironment of the `flink_table`.
    :param flink_table: The input Flink table.
    :param window_descriptor: The descriptor of the sliding window.
    :param agg_descriptors: A list of descriptor that descriptor the aggregation to
                            perform.
    :return: The result table.
    """

    step_interval = timedelta_to_flink_sql_interval(window_descriptor.step_size)
    window_size_interval = timedelta_to_flink_sql_interval(
        window_descriptor.window_size
    )

    if window_descriptor.filter_expr is not None:
        flink_table = flink_table.filter(
            native_flink_expr.call_sql(window_descriptor.filter_expr)
        )
    t_env.create_temporary_view("src_table", flink_table)
    table = t_env.sql_query(
        f"""
        SELECT * FROM TABLE(
            HOP(
               DATA => TABLE src_table,
               TIMECOL => DESCRIPTOR({EVENT_TIME_ATTRIBUTE_NAME}),
               SLIDE => {step_interval},
               SIZE => {window_size_interval}))
        """
    )
    t_env.drop_temporary_view("src_table")

    if window_descriptor.limit is not None:
        table = _window_top_n_by_time(
            t_env,
            table,
            window_descriptor.group_by_keys,
            window_descriptor.limit,
            ascending=False,
        )

    first_value_agg_descriptors = []
    last_value_agg_descriptors = []
    for agg_descriptor in agg_descriptors:
        if agg_descriptor.agg_func == AggFunc.FIRST_VALUE:
            first_value_agg_descriptors.append(agg_descriptor)
        elif agg_descriptor.agg_func == AggFunc.LAST_VALUE:
            last_value_agg_descriptors.append(agg_descriptor)
    filtered_agg_descriptors = filter(
        lambda x: x not in first_value_agg_descriptors
        and x not in last_value_agg_descriptors,
        agg_descriptors,
    )

    first_last_value_table = _get_first_last_value_table(
        t_env,
        table,
        window_descriptor.group_by_keys,
        first_value_agg_descriptors,
        last_value_agg_descriptors,
    )

    group_by_key_cols = [
        native_flink_expr.col(key) for key in window_descriptor.group_by_keys
    ]

    result = table.group_by(
        native_flink_expr.col("window_start"),
        native_flink_expr.col("window_end"),
        native_flink_expr.col("window_time"),
        *group_by_key_cols,
    ).select(
        *group_by_key_cols,
        *[
            _get_sliding_window_agg_select_expr(agg_descriptor)
            for agg_descriptor in filtered_agg_descriptors
        ],
        native_flink_expr.col("window_time").alias(EVENT_TIME_ATTRIBUTE_NAME),
    )

    if first_last_value_table is not None:
        result = join_table_on_key(
            result,
            first_last_value_table,
            [
                *window_descriptor.group_by_keys,
                EVENT_TIME_ATTRIBUTE_NAME,
            ],
        )

    return result


def _get_first_last_value_table(
    t_env: StreamTableEnvironment,
    table: NativeFlinkTable,
    group_by_keys: Sequence[str],
    first_value_agg_descriptors: List["AggregationFieldDescriptor"],
    last_value_agg_descriptors: List["AggregationFieldDescriptor"],
) -> Optional[NativeFlinkTable]:
    first_value_table = None
    last_value_table = None
    if len(first_value_agg_descriptors) != 0:
        first_value_table = _window_top_n_by_time(
            t_env, table, group_by_keys, 1, ascending=True
        ).select(
            *[native_flink_expr.col(key) for key in group_by_keys],
            *[
                native_flink_expr.call_sql(agg_descriptor.expr)
                .cast(agg_descriptor.field_data_type)
                .alias(agg_descriptor.field_name)
                for agg_descriptor in first_value_agg_descriptors
            ],
            native_flink_expr.col("window_time").alias(EVENT_TIME_ATTRIBUTE_NAME),
        )
    if len(last_value_agg_descriptors) != 0:
        last_value_table = _window_top_n_by_time(
            t_env, table, group_by_keys, 1, ascending=False
        ).select(
            *[native_flink_expr.col(key) for key in group_by_keys],
            *[
                native_flink_expr.call_sql(agg_descriptor.expr)
                .cast(agg_descriptor.field_data_type)
                .alias(agg_descriptor.field_name)
                for agg_descriptor in last_value_agg_descriptors
            ],
            native_flink_expr.col("window_time").alias(EVENT_TIME_ATTRIBUTE_NAME),
        )
    if first_value_table is not None and last_value_table is not None:
        return join_table_on_key(
            first_value_table,
            last_value_table,
            [*group_by_keys, EVENT_TIME_ATTRIBUTE_NAME],
        )

    if first_value_table is not None:
        return first_value_table
    elif last_value_table is not None:
        return last_value_table
    else:
        return None


def _get_sliding_window_agg_select_expr(
    agg_descriptor: "AggregationFieldDescriptor",
) -> native_flink_expr:
    expr = native_flink_expr.call_sql(agg_descriptor.expr)
    agg_func = agg_descriptor.agg_func
    result_type = agg_descriptor.field_data_type

    if agg_func == AggFunc.AVG:
        result = expr.avg
    elif agg_func == AggFunc.MIN:
        result = expr.min
    elif agg_func == AggFunc.MAX:
        result = expr.max
    elif agg_func == AggFunc.SUM:
        result = expr.sum
    elif agg_func == AggFunc.COUNT:
        result = expr.count
        result_type = result_type.not_null()
    elif agg_func == AggFunc.VALUE_COUNTS:
        result = native_flink_expr.call(AGG_JAVA_UDF.get(agg_func).udf_name, expr)
    else:
        raise FeathubTransformationException(
            f"Unsupported aggregation for FlinkProcessor {agg_func}."
        )
    return result.cast(result_type).alias(agg_descriptor.field_name)


def _window_top_n_by_time(
    t_env: StreamTableEnvironment,
    table: NativeFlinkTable,
    group_by_keys: Sequence[str],
    n: int,
    ascending: bool = True,
) -> NativeFlinkTable:
    # Top-N by time attribute
    ordering = "ASC" if ascending else "DESC"
    escaped_keys = [f"`{k}`" for k in group_by_keys]

    t_env.create_temporary_view("windowed_table", table)
    table = t_env.sql_query(
        f"""
            SELECT * FROM
               (SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end, window_time,
                        {",".join(escaped_keys)}
                    ORDER BY {EVENT_TIME_ATTRIBUTE_NAME} {ordering})
                AS rownum
                FROM windowed_table)
            WHERE rownum <= {n}
            """
    )
    table = table.drop_columns("rownum")
    t_env.drop_temporary_view("windowed_table")
    return table

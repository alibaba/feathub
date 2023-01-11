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
from copy import deepcopy
from datetime import timedelta
from typing import List, Optional, Sequence

from pyflink.java_gateway import get_gateway
from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
)
from pyflink.table.types import _to_java_data_type, DataTypes

from feathub.common.exceptions import FeathubTransformationException
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureViewConfig,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
    get_default_value_and_type,
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
        step_size: timedelta,
        limit: Optional[int],
        group_by_keys: Sequence[str],
        filter_expr: Optional[str],
    ) -> None:
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
            sliding_window_agg.step_size,
            sliding_window_agg.limit,
            sliding_window_agg.group_by_keys,
            filter_expr,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.step_size == other.step_size
            and self.limit == other.limit
            and self.group_by_keys == other.group_by_keys
            and self.filter_expr == other.filter_expr
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.step_size,
                self.limit,
                tuple(self.group_by_keys),
                self.filter_expr,
            )
        )


# TODO: Retracting the value when the window becomes empty when the Sink support
#  DynamicTable with retraction.
def evaluate_sliding_window_transform(
    t_env: StreamTableEnvironment,
    flink_table: NativeFlinkTable,
    window_descriptor: SlidingWindowDescriptor,
    agg_descriptors: List[AggregationFieldDescriptor],
    config: SlidingFeatureViewConfig,
) -> NativeFlinkTable:
    """
    Evaluate the sliding window transforms on the given flink table and return the
    result table.

    :param t_env: The StreamTableEnvironment of the `flink_table`.
    :param flink_table: The input Flink table.
    :param window_descriptor: The descriptor of the sliding window.
    :param agg_descriptors: A list of descriptor that descriptor the aggregation to
                            perform.
    :param config: The config of the SlidingFeatureView that the window_descriptor
                   belongs to.
    :return: The result table.
    """
    window_sizes = set(
        [agg_descriptor.window_size for agg_descriptor in agg_descriptors]
    )

    if len(window_sizes) == 1:
        flink_table = _apply_sliding_window_with_same_size(
            t_env, flink_table, window_descriptor, window_sizes.pop(), agg_descriptors
        )
        return _apply_post_sliding_window(
            flink_table,
            window_descriptor,
            agg_descriptors,
            config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG),
            config.get(SKIP_SAME_WINDOW_OUTPUT_CONFIG),
        )

    pre_agg_descriptors = []
    avg_fields = []
    for agg_descriptor in agg_descriptors:
        if agg_descriptor.agg_func == AggFunc.AVG:
            avg_fields.append(agg_descriptor.field_name)
            pre_agg_descriptors.append(
                AggregationFieldDescriptor(
                    agg_descriptor.field_name + "_SUM__",
                    agg_descriptor.field_data_type,
                    agg_descriptor.expr,
                    AggFunc.SUM,
                    window_descriptor.step_size,
                )
            )
            pre_agg_descriptors.append(
                AggregationFieldDescriptor(
                    agg_descriptor.field_name + "_COUNT__",
                    DataTypes.BIGINT(),
                    agg_descriptor.expr,
                    AggFunc.COUNT,
                    window_descriptor.step_size,
                )
            )
        else:
            pre_agg_descriptor = deepcopy(agg_descriptor)
            pre_agg_descriptor.window_size = window_descriptor.step_size
            pre_agg_descriptors.append(pre_agg_descriptor)

    flink_table = evaluate_sliding_window_transform(
        t_env,
        flink_table,
        window_descriptor,
        pre_agg_descriptors,
        SlidingFeatureViewConfig(
            {
                **config.original_props,
                ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: False,
                SKIP_SAME_WINDOW_OUTPUT_CONFIG: False,
            }
        ),
    )

    for avg_field in avg_fields:
        flink_table = flink_table.add_columns(
            native_flink_expr.row(
                native_flink_expr.col(avg_field + "_SUM__"),
                native_flink_expr.col(avg_field + "_COUNT__"),
            ).alias(avg_field)
        )
        flink_table = flink_table.drop_columns(avg_field + "_SUM__")
        flink_table = flink_table.drop_columns(avg_field + "_COUNT__")

    table_schema = flink_table.get_schema()

    gateway = get_gateway()
    descriptor_builder = (
        gateway.jvm.com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor.builder()
    )

    for agg_descriptor in agg_descriptors:
        agg_func_name = _get_agg_func_name_after_pre_agg(agg_descriptor.agg_func)
        descriptor_builder.addField(
            agg_descriptor.field_name,
            _to_java_data_type(
                table_schema.get_field_data_type(agg_descriptor.field_name)
            ),
            agg_descriptor.field_name,
            _to_java_data_type(agg_descriptor.field_data_type),
            int(agg_descriptor.window_size.total_seconds() * 1000),
            agg_func_name,
        )

    group_by_keys = gateway.new_array(
        gateway.jvm.String, len(window_descriptor.group_by_keys)
    )
    for idx, key in enumerate(window_descriptor.group_by_keys):
        group_by_keys[idx] = key

    default_row = None
    if config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG):
        default_row = gateway.jvm.org.apache.flink.types.Row.withNames()
        for agg_descriptor in agg_descriptors:
            default_value, data_type = get_default_value_and_type(agg_descriptor)
            default_row.setField(agg_descriptor.field_name, default_value)

    # Java call to apply the SlidingWindowKeyedProcessorFunction to the table, the
    # SlidingWindowKeyedProcessorFunction also takes care of the
    # enable_empty_window_output and skip_same_window_output options
    j_table = gateway.jvm.com.alibaba.feathub.flink.udf.SlidingWindowUtils.applySlidingWindowKeyedProcessFunction(  # noqa
        flink_table._t_env._j_tenv,
        flink_table._j_table,
        group_by_keys,
        EVENT_TIME_ATTRIBUTE_NAME,
        int(window_descriptor.step_size.total_seconds() * 1000),
        descriptor_builder.build(),
        default_row,
        config.get(SKIP_SAME_WINDOW_OUTPUT_CONFIG),
    )
    return NativeFlinkTable(j_table, flink_table._t_env)


def _get_agg_func_name_after_pre_agg(agg_func: AggFunc) -> str:
    """
    Get the aggregation function name used by the Java AggregationFieldDescriptor after
    the table is pre-aggregated. For example, COUNT should be SUM after pre-aggregated.

    :param agg_func: The aggregation function.
    """
    if agg_func == AggFunc.COUNT:
        # We need to use sum aggregation function after the table is pre-aggregated
        # with count aggregation.
        agg_func_name = AggFunc.SUM.value
    elif agg_func == AggFunc.AVG:
        agg_func_name = "ROW_AVG"
    elif agg_func == AggFunc.VALUE_COUNTS:
        agg_func_name = "MERGE_VALUE_COUNTS"
    else:
        agg_func_name = agg_func.value
    return agg_func_name


def _apply_post_sliding_window(
    table: NativeFlinkTable,
    window_descriptor: SlidingWindowDescriptor,
    agg_descriptors: List[AggregationFieldDescriptor],
    enable_empty_window_output: bool,
    skip_same_window_output: bool,
) -> NativeFlinkTable:
    """
    Apply the post sliding window operator to the table. This method should be called
    right after the table is applied the sliding window with the window_descriptor and
    agg_descriptors.

    :param table: The table that just applied with sliding window.
    :param window_descriptor: The window descriptor of the sliding window applied
                              to the table.
    :param agg_descriptors: The aggregation field descriptor of the sliding window
                            applied to the table.
    :param enable_empty_window_output: Whether to enable output on empty window.
    :param skip_same_window_output: Whether to skip the same window output.
    :return: The table after applying the post sliding window operator.
    """
    if enable_empty_window_output:
        gateway = get_gateway()
        object_class = gateway.jvm.String
        key_array = gateway.new_array(
            object_class, len(window_descriptor.group_by_keys)
        )
        for idx, key in enumerate(window_descriptor.group_by_keys):
            key_array[idx] = key
        default_row = gateway.jvm.org.apache.flink.types.Row.withNames()
        for agg_descriptor in agg_descriptors:
            default_value, data_type = get_default_value_and_type(agg_descriptor)
            default_row.setField(agg_descriptor.field_name, default_value)

        # Java call to apply the post sliding operator to the table.
        j_table = gateway.jvm.com.alibaba.feathub.flink.udf.PostSlidingWindowUtils.postSlidingWindow(  # noqa
            table._t_env._j_tenv,
            table._j_table,
            int(window_descriptor.step_size.total_seconds() * 1000),
            default_row,
            skip_same_window_output,
            EVENT_TIME_ATTRIBUTE_NAME,
            key_array,
        )
        table = NativeFlinkTable(j_table, table._t_env)
    else:
        assert skip_same_window_output is False
        # Setting ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG to False and
        # SKIP_SAME_WINDOW_OUTPUT_CONFIG to True is forbidden, and it is checked in
        # SlidingFeatureView.
        # The default behavior the Flink SlidingWindow is the same as
        # ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG == False and
        # SKIP_SAME_WINDOW_OUTPUT_CONFIG == False, so we don't apply the post process
        # function.
    return table


def _apply_sliding_window_with_same_size(
    t_env: StreamTableEnvironment,
    flink_table: NativeFlinkTable,
    window_descriptor: SlidingWindowDescriptor,
    window_size: timedelta,
    agg_descriptors: List[AggregationFieldDescriptor],
) -> NativeFlinkTable:
    """
    When all the aggregations have the same window size, we apply the native Flink
    sliding window to the given flink table.

    :param t_env: The StreamTableEnvironment of the `flink_table`.
    :param flink_table: The input flink table.
    :param window_descriptor: The descriptor of the sliding window to be applied to the
                              input table.
    :param window_size: The window size of the sliding window.
    :param agg_descriptors: The aggregation field descriptor of the sliding window.
    :return:
    """
    step_interval = timedelta_to_flink_sql_interval(window_descriptor.step_size)
    window_size_interval = timedelta_to_flink_sql_interval(window_size)
    if window_descriptor.filter_expr is not None:
        flink_table = flink_table.filter(
            native_flink_expr.call_sql(window_descriptor.filter_expr)
        )
    t_env.create_temporary_view("src_table", flink_table)

    if window_descriptor.step_size == window_size:
        table = t_env.sql_query(
            f"""
            SELECT * FROM TABLE(
                TUMBLE(
                   DATA => TABLE src_table,
                   TIMECOL => DESCRIPTOR({EVENT_TIME_ATTRIBUTE_NAME}),
                   SIZE => {window_size_interval}))
            """
        )
    else:
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
    first_value_agg_descriptors: List[AggregationFieldDescriptor],
    last_value_agg_descriptors: List[AggregationFieldDescriptor],
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

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
from datetime import timedelta
from typing import List, Optional, Sequence

from pyflink.java_gateway import get_gateway
from pyflink.table import (
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    TableSchema as NativeFlinkTableSchema,
)
from pyflink.table.types import DataType, _to_java_data_type
from pyflink.table.window import OverWindowPartitionedOrderedPreceding, Over
from pyflink.util.java_utils import to_jarray

from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.flink_types_utils import (
    cast_field_type_without_changing_nullability,
)
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)


class OverWindowDescriptor:
    """
    Descriptor of an over window.
    """

    def __init__(
        self,
        window_size: Optional[timedelta],
        group_by_keys: Sequence[str],
    ) -> None:
        self.window_size = window_size
        self.group_by_keys = group_by_keys

    @staticmethod
    def from_over_window_transform(
        window_agg_transform: OverWindowTransform,
    ) -> "OverWindowDescriptor":
        return OverWindowDescriptor(
            window_agg_transform.window_size,
            window_agg_transform.group_by_keys,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.window_size == other.window_size
            and self.group_by_keys == other.group_by_keys
        )

    def __hash__(self) -> int:
        return hash((self.window_size, tuple(self.group_by_keys)))


def evaluate_over_window_transform(
    flink_table: NativeFlinkTable,
    window_descriptor: "OverWindowDescriptor",
    agg_descriptors: List["AggregationFieldDescriptor"],
) -> NativeFlinkTable:
    """
    Evaluate the over window transforms on the given flink table and return the
    result table.

    :param flink_table: The input Flink table.
    :param window_descriptor: The descriptor of the over window.
    :param agg_descriptors: A list of descriptor that descriptor the aggregation to
                            perform.
    :return: The result table.
    """
    tmp_table = flink_table
    for agg_descriptor in agg_descriptors:
        tmp_table = tmp_table.add_or_replace_columns(
            native_flink_expr.call_sql(agg_descriptor.expr).alias(
                agg_descriptor.field_name
            )
        )

    window = _get_flink_over_window(window_descriptor)

    result_table = flink_table.over_window(window.alias("w")).select(
        native_flink_expr.col("*"),
        *_get_over_window_agg_column_list(
            tmp_table.get_schema(), window_descriptor, agg_descriptors
        ),
    )
    result_table = cast_field_type_without_changing_nullability(
        result_table,
        {
            descriptor.field_name: descriptor.field_data_type
            for descriptor in agg_descriptors
        },
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

    if over_window_descriptor.window_size is not None:
        return window.preceding(
            native_flink_expr.lit(
                over_window_descriptor.window_size / timedelta(milliseconds=1)
            ).milli
        )

    return window.preceding(native_flink_expr.UNBOUNDED_RANGE)


def _get_over_window_agg_column_list(
    table_schema: NativeFlinkTableSchema,
    window_descriptor: "OverWindowDescriptor",
    agg_descriptors: List["AggregationFieldDescriptor"],
) -> List[native_flink_expr.Expression]:
    return [
        _get_over_window_agg_select_expr(
            descriptor,
            window_descriptor,
            table_schema.get_field_data_type(descriptor.field_name),
            "w",
        ).alias(descriptor.field_name)
        for descriptor in agg_descriptors
    ]


def _get_over_window_agg_select_expr(
    agg_descriptor: AggregationFieldDescriptor,
    over_window_descriptor: "OverWindowDescriptor",
    input_datatype: Optional[DataType],
    window_alias: str,
) -> native_flink_expr.Expression:
    if input_datatype is None:
        raise RuntimeError("Input datatype cannot be None.")

    gateway = get_gateway()
    agg_field_expr = native_flink_expr.call_sql(agg_descriptor.expr)

    if agg_descriptor.filter_expr is not None:
        agg_field_expr = native_flink_expr.row(
            agg_field_expr,
            native_flink_expr.call_sql(agg_descriptor.filter_expr),
        )

    j_agg_func = (
        gateway.jvm.com.alibaba.feathub.flink.udf.OverWindowUtils.getAggregateFunction(
            agg_descriptor.agg_func.value,
            agg_descriptor.limit,
            agg_descriptor.filter_expr,
            over_window_descriptor.window_size is not None,
            _to_java_data_type(input_datatype),
        )
    )

    j_expr = gateway.jvm.org.apache.flink.table.api.Expressions.call(
        j_agg_func,
        to_jarray(
            gateway.jvm.Object,
            [
                agg_field_expr._j_expr_or_property_name,
                native_flink_expr.col(
                    EVENT_TIME_ATTRIBUTE_NAME
                )._j_expr_or_property_name,
            ],
        ),
    )

    return native_flink_expr.Expression(j_expr).over(
        native_flink_expr.col(window_alias)
    )

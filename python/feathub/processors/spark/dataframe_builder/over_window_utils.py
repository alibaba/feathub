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
from typing import Optional, Sequence, List, Dict

from feathub.common.exceptions import FeathubTransformationException, FeathubException
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.spark.dataframe_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)
from feathub.processors.spark.dataframe_builder.spark_sql_expr_utils import (
    to_spark_sql_expr,
)
from pyspark.sql import DataFrame as NativeSparkDataFrame, WindowSpec, Column, functions
from pyspark.sql.window import Window


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
            to_spark_sql_expr(window_agg_transform.filter_expr)
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
    dataframe: NativeSparkDataFrame,
    window_descriptor: "OverWindowDescriptor",
    agg_descriptors: List["AggregationFieldDescriptor"],
) -> NativeSparkDataFrame:

    window_spec = _get_spark_window_spec(window_descriptor)

    if window_descriptor.filter_expr is not None:
        # TODO: Support new behavior of over window filter expression
        raise FeathubTransformationException(
            "Over window with filter expression is not yet supported by SparkProcessor."
        )
    else:
        result_table = dataframe.withColumns(
            _get_over_window_agg_columns(agg_descriptors, window_spec)
        )
    return result_table


def _get_spark_window_spec(
    over_window_descriptor: "OverWindowDescriptor",
) -> WindowSpec:
    if (
        over_window_descriptor.limit is not None
        and over_window_descriptor.window_size is not None
    ):
        # TODO Supports aggregations on both limited and timed window
        raise FeathubException(
            "You cannot set window_size and limit of over window at the same time."
        )

    if len(over_window_descriptor.group_by_keys) == 0:
        window = Window.orderBy(EVENT_TIME_ATTRIBUTE_NAME)
    else:
        window = Window.partitionBy(*over_window_descriptor.group_by_keys).orderBy(
            EVENT_TIME_ATTRIBUTE_NAME
        )

    if over_window_descriptor.limit is not None:
        return window.rowsBetween(1 - over_window_descriptor.limit, Window.currentRow)

    if over_window_descriptor.window_size is not None:
        preceding = int(over_window_descriptor.window_size / timedelta(milliseconds=1))
        return window.rangeBetween(-preceding, Window.currentRow)

    return window.rowsBetween(Window.unboundedPreceding, Window.currentRow)


def _get_over_window_agg_columns(
    agg_descriptors: List["AggregationFieldDescriptor"],
    window_spec: WindowSpec,
) -> Dict[str, Column]:
    return {
        descriptor.field_name: _get_over_window_agg_column(
            descriptor.expr,
            descriptor.agg_func,
            window_spec,
        )
        .cast(descriptor.field_data_type)
        .alias(descriptor.field_name)
        for descriptor in agg_descriptors
    }


def _get_over_window_agg_column(
    expression: str,
    agg_func: AggFunc,
    window_spec: WindowSpec,
) -> Column:
    if agg_func == AggFunc.AVG:
        result = functions.expr(f"avg({expression})")
    elif agg_func == AggFunc.MIN:
        result = functions.expr(f"min({expression})")
    elif agg_func == AggFunc.MAX:
        result = functions.expr(f"max({expression})")
    elif agg_func == AggFunc.SUM:
        result = functions.expr(f"sum({expression})")
    elif agg_func == AggFunc.FIRST_VALUE:
        result = functions.expr(f"first_value({expression})")
    elif agg_func == AggFunc.LAST_VALUE:
        result = functions.expr(f"last_value({expression})")
    elif agg_func == AggFunc.ROW_NUMBER:
        result = functions.row_number()
    elif agg_func == AggFunc.COUNT:
        result = functions.expr(f"count({expression})")
    elif agg_func == AggFunc.VALUE_COUNTS:
        # TODO Adds VALUE_COUNTS support for SparkProcessor
        raise FeathubTransformationException(
            "VALUE_COUNTS is not supported for SparkProcessor currently."
        )
    elif agg_func == AggFunc.COLLECT_LIST:
        # TODO Adds COLLECT_LIST support for SparkProcessor
        raise FeathubTransformationException(
            "COLLECT_LIST is not supported for SparkProcessor currently."
        )
    else:
        raise FeathubTransformationException(
            f"Unsupported aggregation for SparkProcessor {agg_func}."
        )

    return result.over(window_spec)

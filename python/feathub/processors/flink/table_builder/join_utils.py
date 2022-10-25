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
from typing import List, Dict, Tuple, Any, Sequence, Optional

from pyflink.table import (
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    StreamTableEnvironment,
)
from pyflink.table.types import DataType

from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.flink.flink_types_utils import to_flink_type
from feathub.processors.flink.table_builder.aggregation_utils import (
    get_default_value_and_type,
    AggregationFieldDescriptor,
)
from feathub.processors.flink.table_builder.flink_table_builder_constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)
from feathub.table.table_descriptor import TableDescriptor


class JoinFieldDescriptor:
    """
    Descriptor of the join field.
    """

    def __init__(
        self,
        field_name: str,
        field_data_type: Optional[DataType] = None,
        valid_time_interval: Optional[timedelta] = None,
        default_value: Optional[Any] = None,
    ):
        """
        :param field_name: The name of the field to join.
        :param field_data_type: Optional. If it is not None, the field is cast to the
                                given type. Otherwise, use its original type.
        :param valid_time_interval: Optional. If it is not None, it specifies the valid
                                    time period of a value of the field to join. Suppose
                                    the timestamp of the left table is t, if the
                                    timestamp of the right value is within range
                                    [t, t + valid_time_interval), it joined field is set
                                    to the value from right table, otherwise it is set
                                    to the `default_value`.
        :param default_value: The default value of the field when the value is expired.
        """
        self.field_name = field_name
        self.field_data_type = field_data_type
        self.valid_time_interval = valid_time_interval
        self.default_value = default_value

    @staticmethod
    def from_table_descriptor_and_field_name(
        table_descriptor: TableDescriptor, field_name: str
    ) -> "JoinFieldDescriptor":
        feature = table_descriptor.get_feature(field_name)
        transform = feature.transform

        if (
            not isinstance(table_descriptor, SlidingFeatureView)
            or not isinstance(transform, SlidingWindowTransform)
            or table_descriptor.config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG)
        ):
            return JoinFieldDescriptor(field_name, to_flink_type(feature.dtype))

        default_value, _ = get_default_value_and_type(
            agg_descriptor=AggregationFieldDescriptor.from_feature(feature)
        )

        return JoinFieldDescriptor(
            feature.name,
            to_flink_type(feature.dtype),
            transform.step_size,
            default_value,
        )

    @staticmethod
    def from_field_name(field_name: str) -> "JoinFieldDescriptor":
        return JoinFieldDescriptor(field_name)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.field_name == other.field_name
            and self.valid_time_interval == other.valid_time_interval
            and self.default_value == other.default_value
            and self.field_data_type == other.field_data_type
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.field_name,
                self.valid_time_interval,
                self.default_value,
                self.field_data_type,
            )
        )


def join_table_on_key(
    left: NativeFlinkTable, right: NativeFlinkTable, key_fields: List[str]
) -> NativeFlinkTable:
    """
    Join the left and right table on the given key fields.
    """

    right_aliased = _get_field_aliased_right_table(right, key_fields)
    predicate = _get_join_predicate(left, right_aliased, key_fields)

    return left.join(right_aliased, predicate).select(
        *[
            native_flink_expr.col(left_field_name)
            for left_field_name in left.get_schema().get_field_names()
        ],
        *[
            native_flink_expr.col(right_field_name)
            for right_field_name in right.get_schema().get_field_names()
            if right_field_name not in key_fields
        ],
    )


def full_outer_join_on_key_with_default_value(
    left: NativeFlinkTable,
    right: NativeFlinkTable,
    key_fields: List[str],
    field_default_values: Dict[str, Tuple[Any, DataType]],
) -> NativeFlinkTable:
    """
    Full outer join the left and right table on the given key fields. NULL fields after
    join are set to its default value.

    :param left: The left table.
    :param right: The right table.
    :param key_fields: The join keys.
    :param field_default_values: A map that map the field to its default value. If a
                                 field does not exist in the map, its default value is
                                 NULL.
    :return: The joined table.
    """
    right_aliased = _get_field_aliased_right_table(right, key_fields)
    predicate = _get_join_predicate(left, right_aliased, key_fields)

    return left.full_outer_join(right_aliased, predicate).select(
        *[
            native_flink_expr.if_then_else(
                native_flink_expr.col(key).is_not_null,
                native_flink_expr.col(key),
                native_flink_expr.col(f"right.{key}"),
            ).alias(key)
            for key in key_fields
        ],
        *[
            _field_with_default_value_if_null(left_field_name, field_default_values)
            for left_field_name in left.get_schema().get_field_names()
            if left_field_name not in key_fields
        ],
        *[
            _field_with_default_value_if_null(right_field_name, field_default_values)
            for right_field_name in right.get_schema().get_field_names()
            if right_field_name not in key_fields
        ],
    )


def temporal_join(
    t_env: StreamTableEnvironment,
    left: NativeFlinkTable,
    right: NativeFlinkTable,
    keys: Sequence[str],
    right_table_join_field_descriptor: Dict[str, JoinFieldDescriptor],
) -> NativeFlinkTable:
    """
    Temporal join the right table to the left table.

    :param t_env: The StreamTableEnvironment.
    :param left: The left table.
    :param right: The right table.
    :param keys: The join keys.
    :param right_table_join_field_descriptor: A map from right field name to its
                                              JoinFieldDescriptor.
    :return: The joined table.
    """
    t_env.create_temporary_view("left_table", left)
    t_env.create_temporary_view("right_table", right)
    escaped_keys = [f"`{k}`" for k in keys]
    temporal_right_table = t_env.sql_query(
        f"""
    SELECT * FROM (SELECT *,
        ROW_NUMBER() OVER (PARTITION BY {",".join(escaped_keys)}
            ORDER BY `{EVENT_TIME_ATTRIBUTE_NAME}` DESC) AS rownum
        FROM right_table)
    WHERE rownum = 1
    """
    )

    right_aliased = _get_field_aliased_right_table(
        temporal_right_table, [*keys, EVENT_TIME_ATTRIBUTE_NAME]
    )

    t_env.create_temporary_view("temporal_right_table", right_aliased)

    predicates = " and ".join(
        [f"left_table.`{k}` = temporal_right_table.`right.{k}`" for k in keys]
    )

    result_table = t_env.sql_query(
        f"""
    SELECT * FROM left_table LEFT JOIN
        temporal_right_table
        FOR SYSTEM_TIME AS OF left_table.`{EVENT_TIME_ATTRIBUTE_NAME}`
    ON {predicates}
    """
    )

    # If a field has a valid time interval, set it to its default value if it is
    # expired.
    for right_field_name in result_table.get_schema().get_field_names():
        join_field_descriptor = right_table_join_field_descriptor.get(
            right_field_name, None
        )
        if (
            join_field_descriptor is not None
            and join_field_descriptor.valid_time_interval is not None
        ):
            flink_sql_interval = timedelta_to_flink_sql_interval(
                join_field_descriptor.valid_time_interval, day_precision=3
            )
            if join_field_descriptor.default_value is None:
                default_value_expr = native_flink_expr.null_of(
                    join_field_descriptor.field_data_type
                )
            else:
                default_value_expr = native_flink_expr.lit(
                    join_field_descriptor.default_value
                ).cast(join_field_descriptor.field_data_type)
            result_table = result_table.add_or_replace_columns(
                native_flink_expr.if_then_else(
                    native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME)
                    < native_flink_expr.call_sql(
                        f"`right.{EVENT_TIME_ATTRIBUTE_NAME}` + {flink_sql_interval}"
                    ),
                    native_flink_expr.col(right_field_name),
                    default_value_expr,
                ).alias(right_field_name)
            )

    t_env.drop_temporary_view("left_table")
    t_env.drop_temporary_view("right_table")
    t_env.drop_temporary_view("temporal_right_table")
    return result_table


def _get_field_aliased_right_table(
    right: NativeFlinkTable, fields: Sequence[str]
) -> NativeFlinkTable:
    aliased_right_table_field_names = [
        f"right.{f}" if f in fields else f for f in right.get_schema().get_field_names()
    ]
    right_aliased = right.alias(*aliased_right_table_field_names)
    return right_aliased


def _get_join_predicate(
    left: NativeFlinkTable, key_aliased_right: NativeFlinkTable, key_fields: List[str]
) -> native_flink_expr:
    predicates = [
        left.__getattr__(f"{key}") == key_aliased_right.__getattr__(f"right.{key}")
        for key in key_fields
    ]
    if len(predicates) > 1:
        predicate = native_flink_expr.and_(*predicates)
    else:
        predicate = predicates[0]
    return predicate


def _field_with_default_value_if_null(
    field_name: str, field_default_values: Dict[str, Tuple[Any, DataType]]
) -> native_flink_expr:
    if (
        field_name not in field_default_values
        or field_default_values[field_name][0] is None
    ):
        return native_flink_expr.col(field_name)

    return native_flink_expr.if_then_else(
        native_flink_expr.col(field_name).is_not_null,
        native_flink_expr.col(field_name),
        native_flink_expr.lit(field_default_values[field_name][0]).cast(
            field_default_values[field_name][1]
        ),
    ).alias(field_name)

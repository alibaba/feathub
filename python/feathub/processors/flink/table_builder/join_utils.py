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
from typing import List, Dict, Tuple, Any, Sequence, Optional, cast

from pyflink.table import (
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    StreamTableEnvironment,
)
from pyflink.table.types import DataType

from feathub.dsl.expr_utils import (
    is_id,
    get_var_name,
    get_static_map_lookup_variable_and_key,
)
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
    PROCESSING_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.flink_types_utils import (
    to_flink_type,
    cast_field_type_without_changing_nullability,
    to_flink_sql_type,
)
from feathub.processors.flink.table_builder.aggregation_utils import (
    get_default_value_and_type,
    AggregationFieldDescriptor,
)
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    generate_random_table_name,
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
        field_expr: str,
        field_data_type: Optional[DataType] = None,
        valid_time_interval: Optional[timedelta] = None,
        default_value: Optional[Any] = None,
    ):
        """
        :param field_expr: The expression of the field to join.
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
        self.field_expr = field_expr
        self.field_data_type = field_data_type
        self.valid_time_interval = valid_time_interval
        self.default_value = default_value

    @staticmethod
    def from_table_descriptor_and_feature(
        table_descriptor: TableDescriptor, feature: Feature
    ) -> "JoinFieldDescriptor":
        join_expr = cast(JoinTransform, feature.transform).expr
        if is_id(join_expr):
            field_name = get_var_name(join_expr)
        else:
            field_name, _ = get_static_map_lookup_variable_and_key(join_expr)
        transform = table_descriptor.get_feature(field_name).transform

        if (
            not isinstance(table_descriptor, SlidingFeatureView)
            or not isinstance(transform, SlidingWindowTransform)
            or table_descriptor.config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG)
        ):
            return JoinFieldDescriptor(join_expr, to_flink_type(feature.dtype))

        if is_id(join_expr):
            default_value, _ = get_default_value_and_type(
                agg_descriptor=AggregationFieldDescriptor.from_feature(
                    table_descriptor.get_feature(field_name)
                )
            )
        else:
            default_value = None

        return JoinFieldDescriptor(
            join_expr,
            to_flink_type(feature.dtype),
            transform.step_size,
            default_value,
        )

    @staticmethod
    def from_field_name(field_name: str) -> "JoinFieldDescriptor":
        return JoinFieldDescriptor(f"`{field_name}`")

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.field_expr == other.field_expr
            and self.valid_time_interval == other.valid_time_interval
            and self.default_value == other.default_value
            and self.field_data_type == other.field_data_type
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.field_expr,
                self.valid_time_interval,
                self.default_value,
                self.field_data_type,
            )
        )


def join_table_on_key(
    t_env: StreamTableEnvironment,
    left: NativeFlinkTable,
    right: NativeFlinkTable,
    key_fields: List[str],
) -> NativeFlinkTable:
    """
    Join the left and right table on the given key fields.
    """

    # Use SQL instead of Table API to get around the issue in FLINK-32464.

    predicate = _get_join_predicate(key_fields)
    select_statement = ", ".join(
        [
            *[
                f"left_table.`{left_field_name}`"
                for left_field_name in left.get_schema().get_field_names()
            ],
            *[
                f"right_table.`{right_field_name}`"
                for right_field_name in right.get_schema().get_field_names()
                if right_field_name not in key_fields
            ],
        ]
    )

    t_env.create_temporary_view("left_table", left)
    t_env.create_temporary_view("right_table", right)

    result_table = t_env.sql_query(
        f"SELECT {select_statement} FROM left_table JOIN right_table ON {predicate}"
    )

    t_env.drop_temporary_view("left_table")
    t_env.drop_temporary_view("right_table")

    return result_table


def lookup_join(
    t_env: StreamTableEnvironment,
    left: NativeFlinkTable,
    right: NativeFlinkTable,
    key_fields: List[str],
) -> NativeFlinkTable:
    """
    Lookup join the right table to the left table.
    """
    is_processing_time_attribute_appended = False
    if PROCESSING_TIME_ATTRIBUTE_NAME not in left.get_schema().get_field_names():
        is_processing_time_attribute_appended = True
        left = _append_processing_time_attribute(t_env, left)

    t_env.create_temporary_view("left_table", left)
    t_env.create_temporary_view("right_table", right)

    predicates = " and ".join(
        [f"left_table.`{k}` = right_table.`{k}`" for k in key_fields]
    )

    result_table = t_env.sql_query(
        f"""
        SELECT * FROM left_table LEFT JOIN right_table
            FOR SYSTEM_TIME AS OF left_table.`{PROCESSING_TIME_ATTRIBUTE_NAME}`
            ON {predicates};
        """
    )

    t_env.drop_temporary_view("left_table")
    t_env.drop_temporary_view("right_table")

    if is_processing_time_attribute_appended:
        result_table = result_table.drop_columns(
            native_flink_expr.col(PROCESSING_TIME_ATTRIBUTE_NAME)
        )

    return result_table


# TODO: figure out why Flink SQL requires a processing time attribute for lookup join
#  and see if there is space for improvement on Flink API.
def _append_processing_time_attribute(
    t_env: StreamTableEnvironment,
    table: NativeFlinkTable,
) -> NativeFlinkTable:
    tmp_table_name = generate_random_table_name("tmp_table")

    t_env.create_temporary_view(tmp_table_name, table)
    table = t_env.sql_query(
        f"SELECT *, PROCTIME() AS {PROCESSING_TIME_ATTRIBUTE_NAME} "
        f"FROM {tmp_table_name};"
    )
    t_env.drop_temporary_view(tmp_table_name)

    return table


def full_outer_join_on_key_with_default_value(
    t_env: StreamTableEnvironment,
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

    # Use SQL instead of Table API to get around the issue in FLINK-32464.

    predicate = _get_join_predicate(key_fields)

    select_field_exprs = []
    for key in key_fields:
        select_field_exprs.append(
            f"CASE WHEN left_table.`{key}` IS NOT NULL "
            f"THEN left_table.`{key}` "
            f"ELSE right_table.`{key}` "
            f"END AS `{key}`"
        )
    for field_name in left.get_schema().get_field_names():
        if field_name in key_fields:
            continue
        select_field_exprs.append(
            _field_with_default_value_if_null(field_name, field_default_values)
        )
    for field_name in right.get_schema().get_field_names():
        if field_name in key_fields:
            continue
        select_field_exprs.append(
            _field_with_default_value_if_null(field_name, field_default_values)
        )
    select_statement = ", ".join(select_field_exprs)

    t_env.create_temporary_view("left_table", left)
    t_env.create_temporary_view("right_table", right)
    table = t_env.sql_query(
        f"SELECT {select_statement} FROM left_table "
        f"FULL OUTER JOIN right_table ON {predicate};"
    )
    t_env.drop_temporary_view("left_table")
    t_env.drop_temporary_view("right_table")

    return cast_field_type_without_changing_nullability(
        table, {key: value[1] for key, value in field_default_values.items()}
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

    right_aliased = _rename_fields(
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
                )
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
            result_table = cast_field_type_without_changing_nullability(
                result_table, {right_field_name: join_field_descriptor.field_data_type}
            )

    t_env.drop_temporary_view("left_table")
    t_env.drop_temporary_view("right_table")
    t_env.drop_temporary_view("temporal_right_table")
    return result_table


def _rename_fields(right: NativeFlinkTable, fields: Sequence[str]) -> NativeFlinkTable:
    aliased_right_table_field_names = [
        f"right.{f}" if f in fields else f for f in right.get_schema().get_field_names()
    ]
    right_aliased = right.alias(*aliased_right_table_field_names)
    return right_aliased


def _get_join_predicate(key_fields: List[str]) -> str:
    predicates = [f"left_table.`{key}` = right_table.`{key}`" for key in key_fields]
    if len(predicates) > 1:
        predicate = " AND ".join(predicates)
    else:
        predicate = predicates[0]
    return predicate


def _field_with_default_value_if_null(
    field_name: str, field_default_values: Dict[str, Tuple[Any, DataType]]
) -> str:
    if (
        field_name not in field_default_values
        or field_default_values[field_name][0] is None
    ):
        return f"`{field_name}`"

    return (
        f"CASE WHEN `{field_name}` IS NOT NULL THEN `{field_name}` ELSE "
        f"CAST({field_default_values[field_name][0]} AS "
        f"{to_flink_sql_type(field_default_values[field_name][1])}) END "
        f"AS `{field_name}`"
    )

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
import glob
import json
import os
from typing import List, Tuple, Optional, Dict, Sequence

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    expressions as native_flink_expr,
    StatementSet,
)

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.dsl.expr_utils import (
    is_id,
    is_static_map_lookup_op,
    get_static_map_lookup_variable_and_key,
    get_var_name,
)
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.feature_tables.sources.redis_source import (
    NAMESPACE_KEYWORD,
    KEYS_KEYWORD,
    RedisSource,
    FEATURE_NAME_KEYWORD,
    KEY_COLUMN_PREFIX,
)
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.flink_sql_expr_utils import (
    to_flink_sql_expr,
)
from feathub.processors.flink.table_builder.join_utils import (
    JoinFieldDescriptor,
    lookup_join,
)
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    get_schema_from_table,
)
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


def get_redis_source(feature_desc: TableDescriptor) -> Optional[RedisSource]:
    """
    Returns the RedisSource if the feature descriptor represents a RedisSource
    optionally followed by a series of DerivedFeatureView, which is the valid
    syntax before a lookup join using the RedisSource. Returns None if the
    feature descriptor does not match this syntax.
    """
    if isinstance(feature_desc, RedisSource):
        return feature_desc
    if isinstance(feature_desc, DerivedFeatureView):
        if isinstance(feature_desc.source, str):
            raise FeathubException(
                f"feature descriptor {feature_desc.name} must have been resolved."
            )
        return get_redis_source(feature_desc.source)
    return None


def append_physical_key_columns_per_feature(
    table: NativeFlinkTable,
    key_expr: str,
    namespace: str,
    keys: Sequence[str],
    feature_names: List[str],
) -> Tuple[NativeFlinkTable, List[str]]:
    """
    Appends columns containing the physical key values to each feature according to the
    key_expr.

    :return: A Flink Table with the appended columns, and the name of these columns.
    """
    key_expr_template = key_expr.replace(NAMESPACE_KEYWORD, f'"{namespace}"').replace(
        KEYS_KEYWORD, ", ".join(keys)
    )

    physical_key_column_names = []
    for feature_name in feature_names:
        key_expr = key_expr_template.replace(FEATURE_NAME_KEYWORD, f'"{feature_name}"')
        key_column_name = KEY_COLUMN_PREFIX + feature_name
        table = table.add_columns(
            native_flink_expr.call_sql(to_flink_sql_expr(key_expr)).alias(
                key_column_name
            ),
        )
        physical_key_column_names.append(key_column_name)

    return table, physical_key_column_names


def optimize_redis_source_lookup_map(
    t_env: StreamTableEnvironment,
    table: NativeFlinkTable,
    table_descriptor: TableDescriptor,
    join_field_descriptors: Dict[str, JoinFieldDescriptor],
) -> NativeFlinkTable:
    """
    If the right table of a lookup join is a RedisSource, the right table would
    be optimized such that if only designated keys of a map-typed feature is used
    in the join, the RedisSource would only lookup these key fields from the
    Redis hash, instead of reading all hash fields.
    """
    if not isinstance(table_descriptor, RedisSource):
        return table

    fully_acquired_field_names = set()
    hash_fields: Dict[str, List[str]] = dict()
    for descriptor in join_field_descriptors.values():
        if is_id(descriptor.field_expr):
            fully_acquired_field_names.add(get_var_name(descriptor.field_expr))
        elif is_static_map_lookup_op(descriptor.field_expr):
            variable, key = get_static_map_lookup_variable_and_key(
                descriptor.field_expr
            )
            if not isinstance(key, str):
                return table
            if variable not in hash_fields:
                hash_fields[variable] = []
            hash_fields[variable].append(key)
        else:
            return table

    for field_name in fully_acquired_field_names:
        hash_fields.pop(field_name, None)

    if not hash_fields:
        return table

    return get_table_from_redis_source(
        t_env,
        table_descriptor,
        {"hashFields": json.dumps(hash_fields)},
    )


def lookup_join_redis_source(
    t_env: StreamTableEnvironment,
    left_table: NativeFlinkTable,
    right_table: NativeFlinkTable,
    right_table_descriptor: TableDescriptor,
    join_field_descriptors: Dict[str, JoinFieldDescriptor],
    redis_source: RedisSource,
    keys: Sequence[str],
) -> NativeFlinkTable:
    """
    Lookup join the right table to the left table. The right table should be
    derived from a Redis source.

    :param t_env: The table environment.
    :param left_table: The left table.
    :param right_table: The right table.
    :param right_table_descriptor: The right table descriptor.
    :param join_field_descriptors: The join descriptors containing the features
                                   to be joined from the right table.
    :param redis_source: The Redis source that the right table is derived from.
    :param keys: The keys used to join.
    :return: The joined table.
    """
    left_table, physical_keys = append_physical_key_columns_per_feature(
        table=left_table,
        key_expr=redis_source.key_expr,
        namespace=redis_source.namespace,
        keys=redis_source.keys,
        feature_names=[
            x for x in redis_source.schema.field_names if x not in redis_source.keys
        ],
    )

    right_table = optimize_redis_source_lookup_map(
        t_env,
        right_table,
        right_table_descriptor,
        join_field_descriptors,
    )

    select_dict = {
        **{
            field_name: to_flink_sql_expr(join_field_descriptor.field_expr)
            for (
                field_name,
                join_field_descriptor,
            ) in join_field_descriptors.items()
        },
        **{x: f"`{x}`" for x in physical_keys},
    }

    right_table = right_table.select(
        *[
            native_flink_expr.call_sql(field_expr).alias(field_name)
            for field_name, field_expr in select_dict.items()
        ]
    )

    joined_table = lookup_join(
        t_env,
        left_table,
        right_table,
        list(keys) + physical_keys,
    )

    return joined_table.drop_columns(
        *[native_flink_expr.col(field_name) for field_name in physical_keys]
    )


def get_table_from_redis_source(
    t_env: StreamTableEnvironment,
    source: RedisSource,
    extra_props: Optional[Dict[str, str]] = None,
) -> NativeFlinkTable:
    if source.keys is None:
        raise FeathubException("Redis Tables must have keys.")

    add_jar_to_t_env(t_env, *_get_redis_connector_jars())

    feature_field_names = [x for x in source.schema.field_names if x not in source.keys]
    physical_key_field_names = [KEY_COLUMN_PREFIX + x for x in feature_field_names]

    field_names = source.schema.field_names + physical_key_field_names
    field_types = source.schema.field_types + [
        types.String for _ in physical_key_field_names
    ]

    flink_schema = to_flink_schema(Schema(field_names, field_types))

    table_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("redis")
        .schema(flink_schema)
        .option("mode", source.mode.name)
        .option("host", source.host)
        .option("port", str(source.port))
        .option("dbNum", str(source.db_num))
        .option("keyFields", ",".join(source.keys))
    )

    if source.username is not None:
        table_descriptor_builder = table_descriptor_builder.option(
            "username", source.username
        )

    if source.password is not None:
        table_descriptor_builder = table_descriptor_builder.option(
            "password", source.password
        )

    if extra_props is not None:
        for key, value in extra_props.items():
            table_descriptor_builder = table_descriptor_builder.option(key, value)

    return t_env.from_descriptor(table_descriptor_builder.build())


def add_redis_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    features_table: NativeFlinkTable,
    keys: Sequence[str],
    sink: RedisSink,
) -> None:
    if keys is None:
        raise FeathubException("Tables to be materialized to Redis must have keys.")

    add_jar_to_t_env(t_env, *_get_redis_connector_jars())

    if KEYS_KEYWORD not in sink.key_expr and any(
        key not in sink.key_expr for key in keys
    ):
        raise FeathubException(
            f"key_expr {sink.key_expr} does not contain {KEYS_KEYWORD} and all key "
            f"field names. Features saved to Redis might not have unique keys "
            f"and overwrite each other."
        )

    feature_names = [
        x for x in features_table.get_schema().get_field_names() if x not in keys
    ]

    if FEATURE_NAME_KEYWORD not in sink.key_expr and len(feature_names) > 1:
        raise FeathubException(
            "In order to guarantee the uniqueness of feature keys in Redis, "
            f"key_expr {sink.key_expr} should contain {FEATURE_NAME_KEYWORD},"
            f" or the input table should contain only one feature field."
        )

    features_table, _ = append_physical_key_columns_per_feature(
        features_table,
        sink.key_expr,
        sink.namespace,
        keys,
        feature_names,
    )

    features_table = features_table.drop_columns(
        *[native_flink_expr.col(x) for x in keys]
    )

    redis_sink_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("redis")
        .schema(get_schema_from_table(features_table))
        .option("mode", sink.mode.name)
        .option("host", sink.host)
        .option("port", str(sink.port))
        .option("dbNum", str(sink.db_num))
        .option("enableHashPartialUpdate", str(sink.enable_hash_partial_update))
    )

    if sink.username is not None:
        redis_sink_descriptor_builder = redis_sink_descriptor_builder.option(
            "username", sink.username
        )

    if sink.password is not None:
        redis_sink_descriptor_builder = redis_sink_descriptor_builder.option(
            "password", sink.password
        )

    statement_set.add_insert(redis_sink_descriptor_builder.build(), features_table)


def _get_redis_connector_jars() -> list:
    lib_dir = find_jar_lib()
    jar_patterns = [
        "flink-connector-redis-*.jar",
        "jedis-*.jar",
        "gson-*.jar",
        "commons-pool2-*.jar",
    ]
    jars = []
    for x in jar_patterns:
        jars.extend(glob.glob(os.path.join(lib_dir, x)))
    return jars

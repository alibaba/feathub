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
import os
from typing import List, Tuple, Optional

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    expressions as native_flink_expr,
    StatementSet,
)

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.feature_tables.sources.redis_source import (
    NAMESPACE_KEYWORD,
    KEYS_KEYWORD,
    RedisSource,
    FEATURE_NAME_KEYWORD,
    KEY_COLUMN_PREFIX,
)
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.flink_sql_expr_utils import (
    to_flink_sql_expr,
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
    keys: List[str],
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


def get_table_from_redis_source(
    t_env: StreamTableEnvironment,
    source: RedisSource,
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

    return t_env.from_descriptor(table_descriptor_builder.build())


def add_redis_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: RedisSink,
) -> None:
    if features_desc.keys is None:
        raise FeathubException("Tables to be materialized to Redis must have keys.")

    add_jar_to_t_env(t_env, *_get_redis_connector_jars())

    if EVENT_TIME_ATTRIBUTE_NAME in features_table.get_schema().get_field_names():
        features_table = features_table.drop_columns(
            native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME)
        )

    if KEYS_KEYWORD not in sink.key_expr and any(
        key not in sink.key_expr for key in features_desc.keys
    ):
        raise FeathubException(
            f"key_expr {sink.key_expr} does not contain {KEYS_KEYWORD} and all key "
            f"field names. Features saved to Redis might not have unique keys "
            f"and overwrite each other."
        )

    feature_names = [
        x.name
        for x in features_desc.get_output_features()
        if x.name not in features_desc.keys
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
        features_desc.keys,
        feature_names,
    )

    features_table = features_table.drop_columns(
        *[native_flink_expr.col(x) for x in features_desc.keys]
    )

    redis_sink_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("redis")
        .schema(get_schema_from_table(features_table))
        .option("mode", sink.mode.name)
        .option("host", sink.host)
        .option("port", str(sink.port))
        .option("dbNum", str(sink.db_num))
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

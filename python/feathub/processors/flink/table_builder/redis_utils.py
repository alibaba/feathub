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
import glob
import os
from typing import Any, Sequence, List

from pyflink.table import (
    TableResult,
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    ScalarFunction,
    expressions as native_flink_expr,
    DataTypes,
)
from pyflink.table.expressions import col
from pyflink.table.udf import udf

from feathub.common import types
from feathub.common.types import DType
from feathub.common.utils import serialize_object_with_protobuf, to_unix_timestamp
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.flink_types_utils import to_feathub_schema
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    get_schema_from_table,
    generate_random_table_name,
)
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor

# TODO: Add document denoting that users should not declare column names
#  starting and ending with double underscores, which are used as Feathub metadata.
REDIS_SINK_KEY_FIELD_NAME = "__redis_sink_key__"


def insert_into_redis_sink(
    t_env: StreamTableEnvironment,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: RedisSink,
) -> TableResult:
    add_jar_to_t_env(t_env, *_get_redis_connector_jars())

    schema: Schema = to_feathub_schema(features_table.get_schema())
    features_table = _serialize_data_and_convert_key_timestamp(
        features_table,
        schema.field_names,
        schema.field_types,
        features_desc.keys,
        features_desc.timestamp_format,
        features_desc.timestamp_field,
    )

    redis_sink_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("redis")
        .schema(get_schema_from_table(features_table))
        .option("host", sink.host)
        .option("port", str(sink.port))
        .option("dbNum", str(sink.db_num))
        .option("namespace", sink.namespace)
        .option("keyField", REDIS_SINK_KEY_FIELD_NAME)
    )

    if sink.username is not None:
        redis_sink_descriptor_builder = redis_sink_descriptor_builder.option(
            "username", sink.username
        )

    if sink.password is not None:
        redis_sink_descriptor_builder = redis_sink_descriptor_builder.option(
            "password", sink.password
        )

    if features_desc.timestamp_field is not None:
        redis_sink_descriptor_builder = redis_sink_descriptor_builder.option(
            "timestampField", features_desc.timestamp_field
        )

    random_sink_name = generate_random_table_name("RedisSink")
    t_env.create_temporary_table(
        random_sink_name, redis_sink_descriptor_builder.build()
    )
    return features_table.execute_insert(random_sink_name)


def _get_redis_connector_jars() -> list:
    lib_dir = find_jar_lib()
    jar_patterns = [
        "flink-connector-redis-*.jar",
        "jedis-*.jar",
        "gson-*.jar",
    ]
    jars = []
    for x in jar_patterns:
        jars.extend(glob.glob(os.path.join(lib_dir, x)))
    return jars


def _serialize_data_and_convert_key_timestamp(
    flink_table: NativeFlinkTable,
    field_names: Sequence[str],
    field_types: Sequence[DType],
    key_fields: Sequence[str],
    timestamp_format: str,
    timestamp_field: str,
) -> NativeFlinkTable:
    result_fields = []
    for i in range(len(field_names)):
        if field_names[i] == timestamp_field:
            python_udf = udf(
                _TimeStampToEpochMillisFunction(timestamp_format),
                result_type=DataTypes.BIGINT(),
            )
        elif field_names[i] in key_fields:
            continue
        else:
            python_udf = udf(
                _SerializeWithProtobufFunction(field_types[i]),
                result_type=DataTypes.BYTES(),
            )
        result_fields.append(
            native_flink_expr.call(
                python_udf, native_flink_expr.col(field_names[i])
            ).alias(field_names[i])
        )

    flink_table = flink_table.add_or_replace_columns(*result_fields)

    python_udf = udf(
        _AppendJoinedKeyValueFunction(
            *[field_types[field_names.index(x)] for x in key_fields]
        ),
        result_type=DataTypes.BYTES(),
    )

    flink_table = flink_table.select(
        native_flink_expr.col("*"),
        native_flink_expr.call(
            python_udf, *[native_flink_expr.col(x) for x in key_fields]
        ).alias(REDIS_SINK_KEY_FIELD_NAME),
    )

    flink_table = flink_table.drop_columns(*[col(x) for x in key_fields])

    return flink_table


class _TimeStampToEpochMillisFunction(ScalarFunction):
    def __init__(self, timestamp_format: str):
        self.timestamp_format = timestamp_format

    def eval(self, *args: Any) -> Any:
        if self.timestamp_format == "epoch":
            return args[0] * 1000
        if self.timestamp_format == "epoch_millis":
            return args[0]
        return to_unix_timestamp(args[0], self.timestamp_format)


class _SerializeWithProtobufFunction(ScalarFunction):
    def __init__(self, field_type: DType):
        self.field_type = field_type

    def eval(self, *args: Any) -> Any:
        return serialize_object_with_protobuf(args[0], self.field_type)


class _AppendJoinedKeyValueFunction(ScalarFunction):
    def __init__(self, *field_types: DType):
        self.field_types = list(field_types)

    def eval(self, *args: Any) -> Any:
        return serialize_and_join_keys(list(args), self.field_types)


def serialize_and_join_keys(key_objects: List[Any], key_types: List[DType]) -> bytes:
    results = []
    for key_object, key_type in zip(key_objects, key_types):
        results.append(serialize_object_with_protobuf(key_object, key_type))

    if len(results) > 1:
        return serialize_object_with_protobuf(results, types.VectorType(types.Bytes))
    else:
        return results[0]

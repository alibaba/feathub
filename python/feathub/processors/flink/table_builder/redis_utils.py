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

from pyflink.table import (
    TableResult,
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    expressions as native_flink_expr,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    get_schema_from_table,
)
from feathub.table.table_descriptor import TableDescriptor


def insert_into_redis_sink(
    t_env: StreamTableEnvironment,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: RedisSink,
) -> TableResult:
    if features_desc.keys is None:
        raise FeathubException("Tables to be materialized to Redis must have keys.")

    add_jar_to_t_env(t_env, *_get_redis_connector_jars())

    if EVENT_TIME_ATTRIBUTE_NAME in features_table.get_schema().get_field_names():
        features_table = features_table.drop_columns(
            native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME)
        )

    redis_sink_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("redis")
        .schema(get_schema_from_table(features_table))
        .option("mode", sink.mode.name)
        .option("host", sink.host)
        .option("port", str(sink.port))
        .option("dbNum", str(sink.db_num))
        .option("namespace", sink.namespace)
        .option("keyFields", ",".join(features_desc.keys))
    )

    if sink.username is not None:
        redis_sink_descriptor_builder = redis_sink_descriptor_builder.option(
            "username", sink.username
        )

    if sink.password is not None:
        redis_sink_descriptor_builder = redis_sink_descriptor_builder.option(
            "password", sink.password
        )

    return features_table.execute_insert(redis_sink_descriptor_builder.build())


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

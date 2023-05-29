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
from typing import List

from pyflink.table import (
    Table as NativeFlinkTable,
    StreamTableEnvironment,
    TableDescriptor as NativeFlinkTableDescriptor,
    Schema as NativeFlinkSchema,
    StatementSet,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.mysql_sink import MySQLSink
from feathub.processors.flink.flink_jar_utils import add_jar_to_t_env, find_jar_lib


def add_mysql_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    feature_table: NativeFlinkTable,
    sink: MySQLSink,
    keys: List[str],
) -> None:
    add_jar_to_t_env(t_env, _get_jdbc_connector_jar(), _get_mysql_driver_jar())

    table_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("jdbc")
        .option("url", _get_mysql_url(sink.host, sink.port, sink.database))
        .option("table-name", sink.table)
        .option("username", sink.username)
        .option("password", sink.password)
    )

    for key, value in sink.processor_specific_props.items():
        table_descriptor_builder.option(key, value)

    schema_builder = NativeFlinkSchema.new_builder()
    feature_table_schema = feature_table.get_schema()
    for field_name in feature_table_schema.get_field_names():
        data_type = feature_table_schema.get_field_data_type(field_name)
        if field_name in keys:
            data_type = data_type.not_null()
        schema_builder.column(field_name, data_type)
    schema_builder.primary_key(*keys)
    table_descriptor_builder.schema(schema_builder.build())

    statement_set.add_insert(table_descriptor_builder.build(), feature_table)


def _get_mysql_url(host: str, port: int, database: str) -> str:
    return f"jdbc:mysql://{host}:{port}/{database}"


def _get_jdbc_connector_jar() -> str:
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "flink-connector-jdbc-*.jar"))
    if len(jars) < 1:
        raise FeathubException(
            f"Can not find the Flink jdbc connector jar at {lib_dir}."
        )
    return jars[0]


def _get_mysql_driver_jar() -> str:
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "mysql-connector-j-*.jar"))
    if len(jars) < 1:
        raise FeathubException(f"Can not find the mysql driver jar at {lib_dir}.")
    return jars[0]

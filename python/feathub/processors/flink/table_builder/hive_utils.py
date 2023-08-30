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
import random
import string
from typing import Optional, List, Union, Dict

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    SqlDialect,
    StatementSet,
)
from pyflink.table.catalog import HiveCatalog
from pyflink.table.types import DataType

from feathub.common.types import DType
from feathub.feature_tables.sinks.hive_sink import HiveSink
from feathub.feature_tables.sources.hive_source import HiveSource
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.flink_types_utils import to_flink_sql_type
from feathub.processors.flink.table_builder.format_utils import (
    load_format,
    get_flink_format_config,
)

DEFAULT_PROPS = {
    "streaming-source.enable": "true",
    "streaming-source.partition-order": "create-time",
}


def _get_hive_connector_jars() -> list:
    lib_dir = find_jar_lib()
    jar_patterns = [
        "flink-sql-connector-hive-*.jar",
        "flink-connector-hive_*.jar",
        "hive-exec-*.jar",
        "libfb303-*.jar",
        "antlr-runtime-*.jar",
    ]
    jars = []
    for x in jar_patterns:
        jars.extend(glob.glob(os.path.join(lib_dir, x)))
    return jars


_registered_hive_catalog = dict()
_hive_catalog_name_length = 10


def _get_or_register_hive_catalog(
    t_env: StreamTableEnvironment,
    database: str,
    hive_catalog_conf_dir: str,
    hive_catalog_identifier: str,
) -> str:
    key = (hive_catalog_conf_dir, hive_catalog_identifier, database)
    if key not in _registered_hive_catalog:
        catalog_name = "".join(
            random.choice(string.ascii_letters + string.digits)
            for _ in range(_hive_catalog_name_length)
        )
        catalog = HiveCatalog(catalog_name, database, hive_catalog_conf_dir)
        t_env.register_catalog(catalog_name, catalog)
        _registered_hive_catalog[key] = catalog_name
    return _registered_hive_catalog[key]


def _create_table_if_not_exists(
    t_env: StreamTableEnvironment,
    table: str,
    field_names: List[str],
    field_types: List[Union[DType, DataType]],
    processor_specific_props: Optional[Dict[str, str]],
) -> None:
    field_name_type = []
    for field_name, field_type in zip(field_names, field_types):
        field_name_type.append(f"`{field_name}` {to_flink_sql_type(field_type)}")
    schema_sql = ", ".join(field_name_type)

    if processor_specific_props:
        processor_specific_props = {
            **DEFAULT_PROPS,
            **processor_specific_props,
        }
    else:
        processor_specific_props = DEFAULT_PROPS

    property_sql = ", ".join(
        f"'{key}' = '{value}'" for key, value in processor_specific_props.items()
    )
    property_sql = f" TBLPROPERTIES({property_sql})"

    create_table_statement = (
        f"CREATE TABLE IF NOT EXISTS {table} ({schema_sql}){property_sql};"
    )

    dialect = t_env.get_config().get_sql_dialect()
    t_env.get_config().set_sql_dialect(SqlDialect.HIVE)
    t_env.execute_sql(create_table_statement).wait()
    t_env.get_config().set_sql_dialect(dialect)


def get_table_from_hive_source(
    t_env: StreamTableEnvironment,
    source: HiveSource,
) -> NativeFlinkTable:
    add_jar_to_t_env(t_env, *_get_hive_connector_jars())
    hive_catalog_name = _get_or_register_hive_catalog(
        t_env,
        source.database,
        source.hive_catalog_conf_dir,
        source.table_uri["hive_catalog_identifier"],
    )

    load_format(t_env, source.data_format, source.data_format_props)
    processor_specific_props = {"format": source.data_format}

    if source.data_format_props is not None:
        processor_specific_props = {
            **processor_specific_props,
            **get_flink_format_config(source.data_format, source.data_format_props),
        }

    if source.processor_specific_props is not None:
        processor_specific_props = {
            **processor_specific_props,
            **source.processor_specific_props,
        }

    catalog = t_env.get_current_catalog()
    t_env.use_catalog(hive_catalog_name)

    _create_table_if_not_exists(
        t_env=t_env,
        table=source.table,
        field_names=source.schema.field_names,
        field_types=source.schema.field_types,
        processor_specific_props=processor_specific_props,
    )

    table = t_env.from_path(f"{source.table}")
    t_env.use_catalog(catalog)
    return table


def add_hive_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    features_table: NativeFlinkTable,
    sink: HiveSink,
) -> None:
    add_jar_to_t_env(t_env, *_get_hive_connector_jars())
    hive_catalog_name = _get_or_register_hive_catalog(
        t_env,
        sink.database,
        sink.hive_catalog_conf_dir,
        sink.table_uri["hive_catalog_identifier"],
    )

    load_format(t_env, sink.data_format, sink.data_format_props)
    processor_specific_props = {"format": sink.data_format}

    if sink.data_format_props is not None:
        processor_specific_props = {
            **processor_specific_props,
            **get_flink_format_config(sink.data_format, sink.data_format_props),
        }

    if sink.processor_specific_props is not None:
        processor_specific_props = {
            **processor_specific_props,
            **sink.processor_specific_props,
        }

    catalog = t_env.get_current_catalog()
    t_env.use_catalog(hive_catalog_name)

    flink_schema = features_table.get_schema()
    _create_table_if_not_exists(
        t_env=t_env,
        table=sink.table,
        field_names=flink_schema.get_field_names(),
        field_types=flink_schema.get_field_data_types(),
        processor_specific_props=processor_specific_props,
    )

    statement_set.add_insert(sink.table, features_table)
    t_env.use_catalog(catalog)

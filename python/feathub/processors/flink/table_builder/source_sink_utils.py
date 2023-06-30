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

from pyflink.table import (
    expressions as native_flink_expr,
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    StatementSet,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.hive_sink import HiveSink
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sinks.mysql_sink import MySQLSink
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sources.hive_source import HiveSource
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.feature_tables.sources.redis_source import RedisSource
from feathub.processors.flink.table_builder.black_hole_utils import (
    add_black_hole_sink_to_statement_set,
)
from feathub.processors.flink.table_builder.datagen_utils import (
    get_table_from_data_gen_source,
)
from feathub.processors.flink.table_builder.file_system_utils import (
    get_table_from_file_source,
    add_file_sink_to_statement_set,
)
from feathub.processors.flink.table_builder.hive_utils import (
    get_table_from_hive_source,
    add_hive_sink_to_statement_set,
)
from feathub.processors.flink.table_builder.kafka_utils import (
    get_table_from_kafka_source,
    add_kafka_sink_to_statement_set,
)
from feathub.processors.flink.table_builder.mysql_utils import (
    add_mysql_sink_to_statement_set,
)
from feathub.processors.flink.table_builder.print_utils import (
    add_print_sink_to_statement_set,
)
from feathub.processors.flink.table_builder.redis_utils import (
    add_redis_sink_to_statement_set,
    get_table_from_redis_source,
)
from feathub.table.table_descriptor import TableDescriptor


def get_table_from_source(
    t_env: StreamTableEnvironment, source: FeatureTable
) -> NativeFlinkTable:
    """
    Get the Flink Table from the given source.

    :param t_env: The StreamTableEnvironment under which the source table will be
                  created.
    :param source: The source.
    :return: The flink table.
    """
    if isinstance(source, FileSystemSource):
        return get_table_from_file_source(t_env, source)
    elif isinstance(source, KafkaSource):
        return get_table_from_kafka_source(t_env, source, source.keys)
    elif isinstance(source, HiveSource):
        return get_table_from_hive_source(t_env, source)
    elif isinstance(source, RedisSource):
        return get_table_from_redis_source(t_env, source)
    elif isinstance(source, DataGenSource):
        return get_table_from_data_gen_source(t_env, source)
    else:
        raise FeathubException(f"Unsupported source type {type(source)}.")


def add_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: Sink,
) -> None:
    """
    Add an insert operation of the flink table to the given statement set.
    """

    if not sink.keep_timestamp_field and features_desc.timestamp_field is not None:
        features_table = features_table.drop_columns(
            native_flink_expr.col(features_desc.timestamp_field),
        )

    if isinstance(sink, FileSystemSink):
        add_file_sink_to_statement_set(t_env, statement_set, features_table, sink)
    elif isinstance(sink, KafkaSink):
        add_kafka_sink_to_statement_set(
            t_env, statement_set, features_table, sink, features_desc.keys
        )
    elif isinstance(sink, PrintSink):
        add_print_sink_to_statement_set(statement_set, features_table)
    elif isinstance(sink, RedisSink):
        add_redis_sink_to_statement_set(
            t_env, statement_set, features_table, features_desc.keys, sink
        )
    elif isinstance(sink, HiveSink):
        add_hive_sink_to_statement_set(t_env, statement_set, features_table, sink)
    elif isinstance(sink, MySQLSink):
        add_mysql_sink_to_statement_set(
            t_env, statement_set, features_table, sink, features_desc.keys
        )
    elif isinstance(sink, BlackHoleSink):
        add_black_hole_sink_to_statement_set(statement_set, features_table)
    else:
        raise FeathubException(f"Unsupported sink type {type(sink)}.")

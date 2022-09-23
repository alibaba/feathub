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
from typing import Sequence

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableResult,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.processors.flink.table_builder.datagen_utils import (
    get_table_from_data_gen_source,
)
from feathub.processors.flink.table_builder.file_system_utils import (
    get_table_from_file_source,
    insert_into_file_sink,
)
from feathub.processors.flink.table_builder.kafka_utils import (
    get_table_from_kafka_source,
    insert_into_kafka_sink,
)
from feathub.processors.flink.table_builder.print_utils import insert_into_print_sink


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
    elif isinstance(source, DataGenSource):
        return get_table_from_data_gen_source(t_env, source)
    else:
        raise FeathubException(f"Unsupported source type {type(source)}.")


def insert_into_sink(
    t_env: StreamTableEnvironment,
    table: NativeFlinkTable,
    sink: FeatureTable,
    keys: Sequence[str],
) -> TableResult:
    """
    Insert the flink table to the given sink.
    """
    if isinstance(sink, FileSystemSink):
        return insert_into_file_sink(t_env, table, sink)
    elif isinstance(sink, KafkaSink):
        return insert_into_kafka_sink(t_env, table, sink, keys)
    elif isinstance(sink, PrintSink):
        return insert_into_print_sink(t_env, table)
    else:
        raise FeathubException(f"Unsupported sink type {type(sink)}.")

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
    Schema as NativeFlinkSchema,
    TableResult,
)

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.types import DType
from feathub.common.utils import to_java_date_format
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.processors.flink.table_builder.file_system_utils import (
    get_table_from_file_source,
    insert_into_file_sink,
)
from feathub.processors.flink.table_builder.flink_table_builder_constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.table_builder.kafka_utils import (
    get_table_from_kafka_source,
    insert_into_kafka_sink,
)
from feathub.feature_tables.sources.file_system_source import FileSystemSource


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
        return insert_into_file_sink(table, sink)
    elif isinstance(sink, KafkaSink):
        return insert_into_kafka_sink(t_env, table, sink, keys)
    else:
        raise FeathubException(f"Unsupported sink type {type(sink)}.")


def define_watermark(
    flink_schema: NativeFlinkSchema,
    max_out_of_orderness_interval: str,
    timestamp_field: str,
    timestamp_format: str,
    timestamp_field_dtype: DType,
) -> NativeFlinkSchema:
    builder = NativeFlinkSchema.new_builder()
    builder.from_schema(flink_schema)

    if timestamp_format == "epoch":
        if (
            timestamp_field_dtype != types.Int32
            and timestamp_field_dtype != types.Int64
        ):
            raise FeathubException(
                "Timestamp field with epoch format only supports data type of "
                "Int32 and Int64."
            )
        builder.column_by_expression(
            EVENT_TIME_ATTRIBUTE_NAME,
            f"CAST("
            f"  FROM_UNIXTIME(CAST(`{timestamp_field}` AS INTEGER)) "
            f"AS TIMESTAMP(3))",
        )
    else:
        if timestamp_field_dtype != types.String:
            raise FeathubException(
                "Timestamp field with non epoch format only "
                "supports data type of String."
            )
        java_datetime_format = to_java_date_format(timestamp_format).replace(
            "'", "''"  # Escape single quote for sql
        )
        builder.column_by_expression(
            EVENT_TIME_ATTRIBUTE_NAME,
            f"TO_TIMESTAMP(`{timestamp_field}`, '{java_datetime_format}')",
        )

    builder.watermark(
        EVENT_TIME_ATTRIBUTE_NAME,
        watermark_expr=f"`{EVENT_TIME_ATTRIBUTE_NAME}` "
        f"- {max_out_of_orderness_interval}",
    )
    return builder.build()

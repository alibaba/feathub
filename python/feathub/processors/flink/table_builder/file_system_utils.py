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
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    StatementSet,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.format_config import DataFormat
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.format_utils import (
    load_format,
    get_flink_format_config,
)
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    get_schema_from_table,
    define_watermark,
)


def get_table_from_file_source(
    t_env: StreamTableEnvironment, file_source: FileSystemSource
) -> NativeFlinkTable:
    schema = file_source.schema
    if schema is None:
        raise FeathubException(
            "Flink processor requires schema for the FileSystemSource."
        )

    flink_schema = to_flink_schema(schema)

    # Define watermark if the file_source has timestamp field
    if file_source.timestamp_field is not None:
        flink_schema = define_watermark(
            t_env,
            flink_schema,
            file_source.max_out_of_orderness,
            file_source.timestamp_field,
            file_source.timestamp_format,
            schema.get_field_type(file_source.timestamp_field),
        )

    descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("filesystem")
        .format(file_source.data_format)
        .option("path", file_source.path)
        .schema(flink_schema)
    )

    load_format(t_env, file_source.data_format, file_source.data_format_props)
    flink_format_config = get_flink_format_config(
        file_source.data_format, file_source.data_format_props
    )
    for k, v in flink_format_config.items():
        descriptor_builder.option(k, v)

    return t_env.from_descriptor(descriptor_builder.build())


def add_file_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    table: NativeFlinkTable,
    sink: FileSystemSink,
) -> None:
    path = sink.path
    # TODO: Remove this check after FLINK-28513 is resolved.
    if sink.data_format == DataFormat.CSV and path.startswith("s3://"):
        raise FeathubException(
            "Cannot sink files in CSV format to s3 due to FLINK-28513."
        )

    descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("filesystem")
        .schema(get_schema_from_table(table))
        .format(sink.data_format)
        .option("path", path)
    )

    load_format(t_env, sink.data_format, sink.data_format_props)
    flink_format_config = get_flink_format_config(
        sink.data_format, sink.data_format_props
    )
    for k, v in flink_format_config.items():
        descriptor_builder.option(k, v)

    statement_set.add_insert(descriptor_builder.build(), table)

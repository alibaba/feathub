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
from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    TableResult,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)


def get_table_from_file_source(
    t_env: StreamTableEnvironment,
    file_source: FileSystemSource,
    time_attribute: str,
) -> NativeFlinkTable:
    schema = file_source.schema
    if schema is None:
        raise FeathubException(
            "Flink processor requires schema for the FileSystemSource."
        )

    flink_schema = to_flink_schema(schema)

    # Define watermark if the file_source has timestamp field
    if file_source.timestamp_field is not None:
        from feathub.processors.flink.table_builder.source_sink_utils import (
            define_watermark,
        )

        flink_schema = define_watermark(
            flink_schema,
            timedelta_to_flink_sql_interval(
                file_source.max_out_of_orderness, day_precision=3
            ),
            file_source.timestamp_field,
            file_source.timestamp_format,
            schema.get_field_type(file_source.timestamp_field),
            time_attribute,
        )

    descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("filesystem")
        .format(file_source.data_format)
        .option("path", file_source.path)
        .schema(flink_schema)
    )

    if file_source.data_format == "csv":
        # Set ignore-parse-errors to set null in case of csv parse error
        descriptor_builder.option("csv.ignore-parse-errors", "true")

    table = t_env.from_descriptor(descriptor_builder.build())
    return table


def insert_into_file_sink(table: NativeFlinkTable, sink: FileSystemSink) -> TableResult:
    path = sink.path
    return table.execute_insert(
        NativeFlinkTableDescriptor.for_connector("filesystem")
        .format(sink.data_format)
        .option("path", path)
        .build()
    )

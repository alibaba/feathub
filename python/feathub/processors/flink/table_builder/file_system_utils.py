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
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    generate_random_table_name,
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

    if file_source.data_format == "csv":
        # Set ignore-parse-errors to set null in case of csv parse error
        descriptor_builder.option("csv.ignore-parse-errors", "true")

    table_name = generate_random_table_name(file_source.name)
    t_env.create_temporary_table(table_name, descriptor_builder.build())
    return t_env.from_path(table_name)


def insert_into_file_sink(
    t_env: StreamTableEnvironment, table: NativeFlinkTable, sink: FileSystemSink
) -> TableResult:
    path = sink.path

    # TODO: Remove this check after FLINK-28513 is resolved.
    if sink.data_format == "csv" and path.startswith("s3://"):
        raise FeathubException(
            "Cannot sink files in CSV format to s3 due to FLINK-28513."
        )

    # TODO: Alibaba Cloud Realtime Compute has bug that assumes all the tables should
    # have a name in VVR-6.0.2, which should be fixed in next version VVR-6.0.3. As a
    # current workaround, we have to generate a random table name. We should update the
    # code to use anonymous table sink after VVR-6.0.3 is released.
    random_sink_name = generate_random_table_name("FileSink")
    t_env.create_temporary_table(
        random_sink_name,
        NativeFlinkTableDescriptor.for_connector("filesystem")
        .schema(get_schema_from_table(table))
        .format(sink.data_format)
        .option("path", path)
        .build(),
    )
    return table.execute_insert(random_sink_name)

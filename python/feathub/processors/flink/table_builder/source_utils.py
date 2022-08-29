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
    Schema as NativeFlinkSchema,
)

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.types import DType
from feathub.common.utils import to_java_date_format
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)
from feathub.sources.file_source import FileSource


def get_table_from_file_source(
    t_env: StreamTableEnvironment,
    file_source: FileSource,
    time_attribute: str,
) -> NativeFlinkTable:
    """
    Get the Flink Table from the given file source.

    :param t_env: The StreamTableEnvironment under which the source table will be
                  created.
    :param file_source: The FileSource.
    :param time_attribute: The field name of the time attribute.
    :return:
    """
    schema = file_source.schema
    if schema is None:
        raise FeathubException("Flink processor requires schema for the FileSource.")

    flink_schema = to_flink_schema(schema)

    # Define watermark if the file_source has timestamp field
    if file_source.timestamp_field is not None:
        flink_schema = _define_watermark(
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
        .format(file_source.file_format)
        .option("path", file_source.path)
        .schema(flink_schema)
    )

    if file_source.file_format == "csv":
        # Set ignore-parse-errors to set null in case of csv parse error
        descriptor_builder.option("csv.ignore-parse-errors", "true")

    table = t_env.from_descriptor(descriptor_builder.build())
    return table


def _define_watermark(
    flink_schema: NativeFlinkSchema,
    max_out_of_orderness_interval: str,
    timestamp_field: str,
    timestamp_format: str,
    timestamp_field_dtype: DType,
    time_attribute: str,
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
            time_attribute,
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
            time_attribute,
            f"TO_TIMESTAMP(`{timestamp_field}`, '{java_datetime_format}')",
        )

    builder.watermark(
        time_attribute,
        watermark_expr=f"`{time_attribute}` " f"- {max_out_of_orderness_interval}",
    )
    return builder.build()

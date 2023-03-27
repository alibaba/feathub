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
import uuid
from datetime import timedelta

from pyflink.table import (
    Table as NativeFlinkTable,
    Schema as NativeFlinkSchema,
    StreamTableEnvironment,
)

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.types import DType
from feathub.common.utils import to_java_date_format
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)


def generate_random_table_name(original_table_name: str) -> str:
    random_sink_name = f"{original_table_name}_{str(uuid.uuid4()).replace('-', '')}"
    return random_sink_name


def get_schema_from_table(table: NativeFlinkTable) -> NativeFlinkSchema:
    schema_builder = NativeFlinkSchema.new_builder()
    for field_name in table.get_schema().get_field_names():
        schema_builder.column(
            field_name, table.get_schema().get_field_data_type(field_name)
        )
    schema = schema_builder.build()
    return schema


def define_watermark(
    t_env: StreamTableEnvironment,
    flink_schema: NativeFlinkSchema,
    max_out_of_orderness: timedelta,
    timestamp_field: str,
    timestamp_format: str,
    timestamp_field_dtype: DType,
) -> NativeFlinkSchema:
    builder = NativeFlinkSchema.new_builder()
    builder.from_schema(flink_schema)

    if timestamp_field_dtype == types.Timestamp:
        builder.column_by_expression(
            EVENT_TIME_ATTRIBUTE_NAME, f"CAST(`{timestamp_field}` AS TIMESTAMP_LTZ(3))"
        )

    # TODO: Properly handle timestamp with or without timezone.
    elif timestamp_format == "epoch":
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
            f"TO_TIMESTAMP_LTZ(`{timestamp_field}`, 0)",
        )
    elif timestamp_format == "epoch_millis":
        if timestamp_field_dtype != types.Int64:
            raise FeathubException(
                "Timestamp field with epoch format only supports data type of Int64."
            )

        builder.column_by_expression(
            EVENT_TIME_ATTRIBUTE_NAME,
            f"TO_TIMESTAMP_LTZ(`{timestamp_field}`, 3)",
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
            f"TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP_MILLIS(`{timestamp_field}`, "
            f"'{java_datetime_format}', '{t_env.get_config().get_local_timezone()}'), "
            f"3)",
        )

    max_out_of_orderness_interval = timedelta_to_flink_sql_interval(
        max_out_of_orderness + timedelta(milliseconds=1), day_precision=3
    )
    builder.watermark(
        EVENT_TIME_ATTRIBUTE_NAME,
        watermark_expr=f"`{EVENT_TIME_ATTRIBUTE_NAME}` "
        f"- {max_out_of_orderness_interval}",
    )
    return builder.build()

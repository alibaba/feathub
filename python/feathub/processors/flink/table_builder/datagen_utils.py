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
from datetime import timedelta

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
)

from feathub.common.exceptions import FeathubException
from feathub.common.types import Timestamp, String
from feathub.feature_tables.sources.datagen_source import (
    DataGenSource,
    RandomField,
    SequenceField,
)
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    define_watermark,
    generate_random_table_name,
)


def get_table_from_data_gen_source(
    t_env: StreamTableEnvironment, data_gen_source: DataGenSource
) -> NativeFlinkTable:
    flink_schema = to_flink_schema(data_gen_source.schema)

    # Define watermark if the kafka_source has timestamp field
    if data_gen_source.timestamp_field is not None:
        flink_schema = define_watermark(
            t_env,
            flink_schema,
            data_gen_source.max_out_of_orderness,
            data_gen_source.timestamp_field,
            data_gen_source.timestamp_format,
            data_gen_source.schema.get_field_type(data_gen_source.timestamp_field),
        )

    table_descriptor_builder = NativeFlinkTableDescriptor.for_connector(
        "datagen"
    ).schema(flink_schema)

    table_descriptor_builder.option(
        "rows-per-second", str(data_gen_source.rows_per_second)
    )

    if data_gen_source.number_of_rows is not None:
        table_descriptor_builder.option(
            "number-of-rows", str(data_gen_source.number_of_rows)
        )

    for field, field_config in data_gen_source.field_configs.items():
        if isinstance(field_config, RandomField):
            table_descriptor_builder.option(f"fields.{field}.kind", "random")
            if field_config.minimum is not None:
                table_descriptor_builder.option(
                    f"fields.{field}.min", str(field_config.minimum)
                )
            if field_config.maximum is not None:
                table_descriptor_builder.option(
                    f"fields.{field}.max", str(field_config.maximum)
                )
            if data_gen_source.schema.get_field_type(field) == Timestamp:
                table_descriptor_builder.option(
                    f"fields.{field}.max-past",
                    f"{field_config.max_past // timedelta(milliseconds=1)} ms",
                )
            if data_gen_source.schema.get_field_type(field) == String:
                table_descriptor_builder.option(
                    f"fields.{field}.length", str(field_config.length)
                )
        elif isinstance(field_config, SequenceField):
            table_descriptor_builder.option(f"fields.{field}.kind", "sequence")
            if field_config.start is not None:
                table_descriptor_builder.option(
                    f"fields.{field}.start", str(field_config.start)
                )
            if field_config.end is not None:
                table_descriptor_builder.option(
                    f"fields.{field}.end", str(field_config.end)
                )
        else:
            raise FeathubException(f"Unknown field config type {type(field_config)}.")

    table_name = generate_random_table_name(data_gen_source.name)
    t_env.create_temporary_table(table_name, table_descriptor_builder.build())
    return t_env.from_path(table_name)

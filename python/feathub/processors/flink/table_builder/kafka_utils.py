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
from datetime import timedelta
from typing import Sequence

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    Schema,
    DataTypes,
    StatementSet,
)

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.utils import to_java_date_format
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.format_utils import (
    load_format,
    get_flink_format_config,
)
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    define_watermark,
    get_schema_from_table,
)
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)


def get_table_from_kafka_source(
    t_env: StreamTableEnvironment,
    kafka_source: KafkaSource,
    keys: Sequence[str],
) -> NativeFlinkTable:
    add_jar_to_t_env(
        t_env, _get_kafka_connector_jar(), _get_bounded_kafka_connector_jar()
    )
    schema = kafka_source.schema
    if schema is None:
        raise FeathubException("Flink processor requires schema for the KafkaSource.")

    flink_schema = to_flink_schema(schema)

    # Define watermark if the kafka_source has timestamp field
    if kafka_source.timestamp_field is not None:
        if (
            kafka_source.timestamp_format == "epoch"
            or kafka_source.timestamp_format == "epoch_millis"
        ):
            flink_schema = define_watermark(
                t_env,
                flink_schema,
                kafka_source.max_out_of_orderness,
                kafka_source.timestamp_field,
                kafka_source.timestamp_format,
                schema.get_field_type(kafka_source.timestamp_field),
            )
        else:
            # TODO: Kafka Source throw exception when define a UDF computed column as
            #  row time attribute. We only compute the timestamp here without defining
            #  the row time attribute. The table will then be converted to DataStream
            #  and back to Table. Its row time attribute is defined when it is
            #  converted back to Table. This logical can be remove after UNIX_TIMESTAMP
            #  support return millisecond epoch.
            #  https://issues.apache.org/jira/browse/FLINK-19200
            builder = Schema.new_builder().from_schema(flink_schema)
            if schema.get_field_type(kafka_source.timestamp_field) != types.String:
                raise FeathubException(
                    "Timestamp field with non epoch format only "
                    "supports data type of String."
                )
            java_datetime_format = to_java_date_format(
                kafka_source.timestamp_format
            ).replace(
                "'", "''"  # Escape single quote for sql
            )
            builder.column_by_expression(
                EVENT_TIME_ATTRIBUTE_NAME,
                f"TO_TIMESTAMP_LTZ("
                f"UNIX_TIMESTAMP_MILLIS(`{kafka_source.timestamp_field}`, "
                f"'{java_datetime_format}', "
                f"'{t_env.get_config().get_local_timezone()}'), 3)",
            )
            flink_schema = builder.build()

    connector_type = "bounded-kafka" if kafka_source.is_bounded() else "kafka"
    descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector(connector_type)
        .option("value.format", kafka_source.value_format)
        .option("properties.bootstrap.servers", kafka_source.bootstrap_server)
        .option("topic", kafka_source.topic)
        .option("properties.group.id", kafka_source.consumer_group)
        .option(
            "scan.topic-partition-discovery.interval",
            f"{kafka_source.partition_discovery_interval // timedelta(milliseconds=1)} "
            f"ms",
        )
        .schema(flink_schema)
    )

    load_format(t_env, kafka_source.value_format, kafka_source.value_data_format_props)
    flink_value_format_config = get_flink_format_config(
        kafka_source.value_format, kafka_source.value_data_format_props
    )
    for k, v in flink_value_format_config.items():
        descriptor_builder.option(f"value.{k}", v)
    if kafka_source.key_format is not None and len(keys) > 0:
        load_format(t_env, kafka_source.key_format, kafka_source.key_data_format_props)
        flink_key_format_config = get_flink_format_config(
            kafka_source.key_format, kafka_source.key_data_format_props
        )
        for k, v in flink_key_format_config.items():
            descriptor_builder.option(f"key.{k}", v)
        descriptor_builder.option("key.format", kafka_source.key_format)
        descriptor_builder.option("key.fields", ";".join(keys))
        descriptor_builder.option("value.fields-include", "EXCEPT_KEY")

    descriptor_builder.option("scan.startup.mode", kafka_source.startup_mode)
    if kafka_source.startup_mode == "timestamp":
        if kafka_source.startup_datetime is None:
            raise FeathubException(
                "startup_datetime cannot be None when startup_mode is timestamp."
            )
        descriptor_builder.option(
            "scan.startup.timestamp-millis",
            str(int(kafka_source.startup_datetime.timestamp() * 1000)),
        )

    for k, v in kafka_source.consumer_props.items():
        descriptor_builder.option(f"properties.{k}", v)

    table = t_env.from_descriptor(descriptor_builder.build())

    if (
        kafka_source.timestamp_format != "epoch"
        and kafka_source.timestamp_format != "epoch_millis"
    ):
        # TODO: Define row time attribute when converted from DataStream. This logical
        #  can be remove after UNIX_TIMESTAMP support return
        #  millisecond epoch.
        #  https://issues.apache.org/jira/browse/FLINK-19200
        schema = to_flink_schema(schema)
        max_out_of_orderness_interval = timedelta_to_flink_sql_interval(
            kafka_source.max_out_of_orderness + timedelta(milliseconds=1),
            day_precision=3,
        )
        schema = (
            Schema.new_builder()
            .from_schema(schema)
            .column(EVENT_TIME_ATTRIBUTE_NAME, DataTypes.TIMESTAMP_LTZ(3))
            .watermark(
                EVENT_TIME_ATTRIBUTE_NAME,
                watermark_expr=f"`{EVENT_TIME_ATTRIBUTE_NAME}` "
                f"- {max_out_of_orderness_interval}",
            )
            .build()
        )
        table = t_env.from_data_stream(t_env.to_data_stream(table), schema)

    return table


def add_kafka_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    table: NativeFlinkTable,
    sink: KafkaSink,
    keys: Sequence[str],
) -> None:
    add_jar_to_t_env(
        t_env, _get_kafka_connector_jar(), _get_bounded_kafka_connector_jar()
    )
    bootstrap_server = sink.bootstrap_server
    topic = sink.topic
    kafka_sink_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("kafka")
        .schema(get_schema_from_table(table))
        .option("value.format", sink.value_format)
        .option("properties.bootstrap.servers", bootstrap_server)
        .option("topic", topic)
    )

    load_format(t_env, sink.value_format, sink.value_format_props)
    flink_value_format_config = get_flink_format_config(
        sink.value_format, sink.value_format_props
    )
    for k, v in flink_value_format_config.items():
        kafka_sink_descriptor_builder.option(f"value.{k}", v)
    if sink.key_format is not None and len(keys) > 0:
        load_format(t_env, sink.key_format, sink.key_format_props)
        flink_key_format_config = get_flink_format_config(
            sink.key_format, sink.key_format_props
        )
        for k, v in flink_key_format_config.items():
            kafka_sink_descriptor_builder.option(f"key.{k}", v)
        kafka_sink_descriptor_builder.option("value.fields-include", "EXCEPT_KEY")
        kafka_sink_descriptor_builder.option("key.format", sink.key_format)
        kafka_sink_descriptor_builder.option("key.fields", ";".join(keys))

    for k, v in sink.producer_props.items():
        kafka_sink_descriptor_builder.option(f"properties.{k}", v)

    statement_set.add_insert(kafka_sink_descriptor_builder.build(), table)


def _get_kafka_connector_jar() -> str:
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "flink-sql-connector-kafka-*.jar"))
    if len(jars) < 1:
        raise FeathubException(
            f"Can not find the Flink Kafka connector jar at {lib_dir}."
        )
    return jars[0]


def _get_bounded_kafka_connector_jar() -> str:
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "flink-connector-kafka-*.jar"))
    if len(jars) < 1:
        raise FeathubException(
            f"Can not find the Flink Kafka connector jar at {lib_dir}."
        )
    return jars[0]

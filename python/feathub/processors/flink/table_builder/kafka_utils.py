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
import glob
import os
from datetime import timedelta
from typing import Sequence

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    TableResult,
    Schema,
    DataTypes,
)

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.utils import to_java_date_format
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.flink_table_builder_constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    define_watermark,
    generate_random_table_name,
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
    if kafka_source.key_format is not None and len(keys) > 0:
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

    for k, v in kafka_source.consumer_properties.items():
        descriptor_builder.option(f"properties.{k}", v)

    # Set ignore-parse-errors to set null in case of csv parse error
    if kafka_source.value_format == "csv":
        descriptor_builder.option("value.csv.ignore-parse-errors", "true")
    if kafka_source.key_format == "csv":
        descriptor_builder.option("key.csv.ignore-parse-errors", "true")

    table_name = generate_random_table_name(kafka_source.name)
    t_env.create_temporary_table(table_name, descriptor_builder.build())
    table = t_env.from_path(table_name)

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


def insert_into_kafka_sink(
    t_env: StreamTableEnvironment,
    table: NativeFlinkTable,
    sink: KafkaSink,
    keys: Sequence[str],
) -> TableResult:
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

    if sink.key_format is not None and len(keys) > 0:
        kafka_sink_descriptor_builder.option("value.fields-include", "EXCEPT_KEY")
        kafka_sink_descriptor_builder.option("key.format", sink.key_format)
        kafka_sink_descriptor_builder.option("key.fields", ";".join(keys))

    for k, v in sink.producer_properties.items():
        kafka_sink_descriptor_builder.option(f"properties.{k}", v)

    # TODO: Alibaba Cloud Realtime Compute has bug that assumes all the tables should
    # have a name in VVR-6.0.2, which should be fixed in next version VVR-6.0.3. As a
    # current workaround, we have to generate a random table name. We should update the
    # code to use anonymous table sink after VVR-6.0.3 is released.
    random_sink_name = generate_random_table_name("KafkaSink")
    t_env.create_temporary_table(
        random_sink_name, kafka_sink_descriptor_builder.build()
    )
    return table.execute_insert(random_sink_name)


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

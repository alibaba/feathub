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
from concurrent.futures import Executor, Future

from pyspark import Row
from pyspark.sql import DataFrame as NativeSparkDataFrame, SparkSession

from feathub.common.exceptions import FeathubException
from feathub.common.utils import get_table_schema
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.memory_store_sink import MemoryStoreSink
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.spark.dataframe_builder.datagen_utils import (
    get_dataframe_from_data_gen_source,
)
from feathub.processors.spark.dataframe_builder.time_utils import (
    append_unix_time_attribute_column,
)
from feathub.processors.spark.spark_types_utils import to_spark_struct_type
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


def get_dataframe_from_source(
    spark_session: SparkSession, source: FeatureTable
) -> NativeSparkDataFrame:
    """
    Get the Spark DataFrame from the given source.

    :param spark_session: The SparkSession where the source table will be created.
    :param source: The Feature Table describing the source.

    :return: The Spark DataFrame.
    """
    if isinstance(source, FileSystemSource):
        source_dataframe = (
            spark_session.read.format(source.data_format)
            .schema(to_spark_struct_type(source.schema))
            .load(source.path)
        )
        source_dataframe = (
            source_dataframe
            if source.timestamp_field is None
            else append_unix_time_attribute_column(
                source_dataframe,
                source.timestamp_field,
                source.timestamp_format,
            )
        )
        return source_dataframe
    elif isinstance(source, DataGenSource):
        return get_dataframe_from_data_gen_source(spark_session, source)
    else:
        raise FeathubException(f"Unsupported source type {type(source)}.")


def insert_into_sink(
    executor: Executor,
    dataframe: NativeSparkDataFrame,
    features_desc: TableDescriptor,
    sink: Sink,
    allow_overwrite: bool,
) -> Future:
    """
    Insert the Spark DataFrame to the given sink. The process would be executed
    asynchronously by the provided executor.

    :param executor: The executor to handle the execution of Spark jobs
                     asynchronously.
    :param dataframe: The Spark DataFrame to be inserted into a sink.
    :param features_desc: The descriptor for the features to be inserted.
    :param sink: The FeatureTable describing the sink.
    :param allow_overwrite: If it is false, throw error if the features collide with
                            existing data in the given sink.

    :return: The Future holding the asynchronously executed Spark job.
    """

    if isinstance(sink, FileSystemSink):
        writer = dataframe.write
        writer = writer.format(sink.data_format)

        if allow_overwrite:
            writer = writer.mode("overwrite")
        else:
            writer = writer.mode("error")

        future = executor.submit(writer.save, path=sink.path)
    elif isinstance(sink, PrintSink):
        future = executor.submit(dataframe.show)
    elif isinstance(sink, BlackHoleSink):

        def nop(_: Row) -> None:
            pass

        future = executor.submit(dataframe.foreach, f=nop)
    elif isinstance(sink, MemoryStoreSink):
        future = executor.submit(
            _write_features_to_online_store,
            dataframe=dataframe,
            features_desc=features_desc,
            schema=get_table_schema(features_desc),
            sink=sink,
        )
    else:
        raise FeathubException(f"Unsupported sink type {type(sink)}.")

    return future


def _write_features_to_online_store(
    dataframe: NativeSparkDataFrame,
    features_desc: TableDescriptor,
    schema: Schema,
    sink: MemoryStoreSink,
) -> None:
    MemoryOnlineStore.get_instance().put(
        table_name=sink.table_name,
        features=dataframe.toPandas(),
        schema=schema,
        key_fields=features_desc.keys,
        timestamp_field=features_desc.timestamp_field,
        timestamp_format=features_desc.timestamp_format,
    )

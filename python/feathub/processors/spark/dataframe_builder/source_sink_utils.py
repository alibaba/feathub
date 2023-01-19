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
from concurrent.futures import Executor, Future

from pyspark import Row
from pyspark.sql import DataFrame as NativeSparkDataFrame, SparkSession

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.processors.spark.dataframe_builder.datagen_utils import (
    get_dataframe_from_data_gen_source,
)
from feathub.processors.spark.spark_types_utils import to_spark_struct_type


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
        return (
            spark_session.read.format(source.data_format)
            .schema(to_spark_struct_type(source.schema))
            .load(source.path)
        )
    elif isinstance(source, DataGenSource):
        return get_dataframe_from_data_gen_source(spark_session, source)
    else:
        raise FeathubException(f"Unsupported source type {type(source)}.")


def insert_into_sink(
    executor: Executor,
    dataframe: NativeSparkDataFrame,
    sink: FeatureTable,
    allow_overwrite: bool,
) -> Future:
    """
    Insert the Spark DataFrame to the given sink. The process would be executed
    asynchronously by the provided executor.

    :param executor: The executor to handle the execution of Spark jobs
                     asynchronously.
    :param dataframe: The Spark DataFrame to be inserted into a sink.
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
    else:
        raise FeathubException(f"Unsupported sink type {type(sink)}.")

    return future

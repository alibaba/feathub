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
import logging
import pickle
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    TableDescriptor as NativeFlinkTableDescriptor,
)

from feathub.common.exceptions import FeathubException
from feathub.processors.flink.table_builder.flink_table_builder import (
    FlinkTableBuilder,
)
from feathub.processors.flink.job_submitter.feathub_job_descriptor import (
    FeathubJobDescriptor,
)
from feathub.registries.registry import Registry
from feathub.feature_tables.sinks.file_sink import FileSink

logger = logging.getLogger(__file__)


def run_job(feathub_job_descriptor_path: str) -> None:
    """
    Run the Feathub job with the given feathub job descriptor.

    The method is expected to run at the JobManager in a remote Flink cluster as the
    Flink job entry point. It parses the FeathubJobDescriptor, including FlinkTable,
    Sink, etc., from the given feathub_job_path and execute the Flink job that computes
    features in table and inserts into the given Sink.

    :param feathub_job_descriptor_path: The path of the Feathub job config.
    """
    with open(feathub_job_descriptor_path, "rb") as f:
        feathub_job_descriptor: FeathubJobDescriptor = pickle.load(f)
    logger.info(f"Loaded Feathub job config: {str(feathub_job_descriptor)}")

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    registry_config = feathub_job_descriptor.registry_config
    registry = Registry.instantiate(
        registry_type=feathub_job_descriptor.registry_type, config=registry_config
    )

    for _, join_table in feathub_job_descriptor.local_registry_tables.items():
        registry.register_features(features=join_table, override=True)

    flink_table_builder = FlinkTableBuilder(
        t_env=t_env,
        registry=registry,
    )

    if isinstance(feathub_job_descriptor.sink, FileSink):
        path = feathub_job_descriptor.sink.path
        native_flink_table = flink_table_builder.build(
            features=feathub_job_descriptor.features,
            keys=feathub_job_descriptor.keys,
            start_datetime=feathub_job_descriptor.start_datetime,
            end_datetime=feathub_job_descriptor.end_datetime,
        )
        table_result = native_flink_table.execute_insert(
            NativeFlinkTableDescriptor.for_connector("filesystem")
            .format(feathub_job_descriptor.sink.data_format)
            .option("path", path)
            .build()
        )
    else:
        raise FeathubException(
            f"Unsupported sink type {type(feathub_job_descriptor.sink)}."
        )

    table_result.wait()


if __name__ == "__main__":
    try:
        run_job(feathub_job_descriptor_path=sys.argv[1])
    except BaseException as e:
        logger.error("Failed to run job", exc_info=e)

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
import logging
import sys

import cloudpickle
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
)

from feathub.common.exceptions import FeathubException
from feathub.processors.flink.flink_class_loader_utils import (
    get_flink_context_class_loader,
)
from feathub.processors.flink.job_submitter.feathub_job_descriptor import (
    FeathubJobDescriptor,
)
from feathub.processors.flink.table_builder.flink_table_builder import (
    FlinkTableBuilder,
)
from feathub.processors.flink.table_builder.source_sink_utils import (
    add_sink_to_statement_set,
)
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor

logger = logging.getLogger(__file__)


def run_job(feathub_job_descriptor_path: str) -> None:
    """
    Run the FeatHub job with the given feathub job descriptor.

    The method is expected to run at the JobManager in a remote Flink cluster as the
    Flink job entry point. It parses the FeathubJobDescriptor, including FlinkTable,
    Sink, etc., from the given feathub_job_path and execute the Flink job that computes
    features in table and inserts into the given Sink.

    :param feathub_job_descriptor_path: The path of the FeatHub job config.
    """
    with open(feathub_job_descriptor_path, "rb") as f:
        feathub_job_descriptor: FeathubJobDescriptor = cloudpickle.load(f)
    logger.info(f"Loaded FeatHub job config: {str(feathub_job_descriptor)}")

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    props = feathub_job_descriptor.props
    registry = Registry.instantiate(props=props)

    for _, join_table in feathub_job_descriptor.local_registry_tables.items():
        registry.register_features(feature_descriptors=[join_table], force_update=True)

    flink_table_builder = FlinkTableBuilder(
        t_env=t_env,
        class_loader=get_flink_context_class_loader(),
        registry=registry,
    )

    statement_set = t_env.create_statement_set()
    for (
        materialization_descriptor
    ) in feathub_job_descriptor.materialization_descriptors:
        feature_descriptor = materialization_descriptor.feature_descriptor
        if not isinstance(feature_descriptor, TableDescriptor):
            raise FeathubException(
                f"The FeatureDescriptor is unresolved: {feature_descriptor}."
            )
        native_flink_table = flink_table_builder.build(
            features=feature_descriptor,
            keys=None,
            start_datetime=materialization_descriptor.start_datetime,
            end_datetime=materialization_descriptor.end_datetime,
        )
        add_sink_to_statement_set(
            t_env=t_env,
            statement_set=statement_set,
            features_table=native_flink_table,
            features_desc=feature_descriptor,
            sink=materialization_descriptor.sink,
        )

    statement_set.execute().wait()


if __name__ == "__main__":
    try:
        run_job(feathub_job_descriptor_path=sys.argv[1])
    except BaseException as e:
        logger.error("Failed to run job", exc_info=e)

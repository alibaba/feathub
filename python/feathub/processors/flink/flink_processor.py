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
import os
from datetime import datetime
from typing import Optional, Union, Dict, Tuple, Sequence
from urllib.parse import urlparse

import pandas as pd
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
)

from feathub.common.config import TIMEZONE_CONFIG
from feathub.common.exceptions import (
    FeathubException,
    FeathubConfigurationException,
)
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.sql_feature_view import SqlFeatureView
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.processors.flink.flink_class_loader_utils import (
    ClassLoader,
    get_flink_context_class_loader,
)
from feathub.processors.flink.flink_deployment_mode import DeploymentMode
from feathub.processors.flink.flink_processor_config import (
    FlinkProcessorConfig,
    DEPLOYMENT_MODE_CONFIG,
    MASTER_CONFIG,
    NATIVE_CONFIG_PREFIX,
    NATIVE_CONFIG_PROCESSOR_CONFIG_MAP,
)
from feathub.processors.flink.flink_table import FlinkTable
from feathub.processors.flink.job_submitter.flink_job_submitter import FlinkJobSubmitter
from feathub.processors.flink.job_submitter.flink_session_cluster_job_submitter import (
    FlinkSessionClusterJobSubmitter,
)
from feathub.processors.flink.table_builder.flink_table_builder import (
    FlinkTableBuilder,
)
from feathub.processors.processor import (
    Processor,
)
from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.processors.processor_job import ProcessorJob
from feathub.registries.local_registry import LocalRegistry
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor

logger = logging.getLogger(__file__)


class FlinkProcessor(Processor):
    """
    The FlinkProcessor does feature ETL using Flink as the processing engine.

    In the following we describe the keys accepted by the `config` dict passed to the
    FlinkProcessor constructor. Note that the accepted config keys depend on the
    `deployment_mode` of the FlinkProcessor.

    Config keys accepted by all deployment modes:
        deployment_mode: The flink job deployment mode, it could be "cli" or "session".
                         Default to "session".
        native.*: Any key with the "native" prefix will be forwarded to the Flink job
                  config after the "native" prefix is removed. For example, if the
                  processor config has an entry "native.parallelism.default: 2",
                  then the Flink job config will have an entry "parallelism.default: 2".

    Extra config keys accepted when deployment_mode = "session":
        master: The Flink JobManager URL to connect to. Required.
    """

    def __init__(self, props: Dict, registry: Registry) -> None:
        """
        Instantiate the FlinkProcessor.

        :param props: The processor properties.
        :param registry: An entity registry.
        """
        super().__init__()
        self.config = FlinkProcessorConfig(props)
        self.registry = registry

        try:
            self.deployment_mode = DeploymentMode(
                self.config.get(DEPLOYMENT_MODE_CONFIG)
            )
        except ValueError:
            raise FeathubException("Unsupported deployment mode.")

        if self.deployment_mode == DeploymentMode.CLI or (
            self.deployment_mode == DeploymentMode.SESSION
            and self.config.get(MASTER_CONFIG) == "local"
        ):
            t_env, class_loader = self._get_table_env()
            self.flink_table_builder = FlinkTableBuilder(
                t_env, class_loader, self.registry
            )

            # The type of flink_job_submitter is set explicitly to the base class
            # FlinkJobSubmitter so that the type checking won't complain.
            self.flink_job_submitter: FlinkJobSubmitter = (
                FlinkSessionClusterJobSubmitter(self)
            )
        elif self.deployment_mode == DeploymentMode.SESSION:
            master = self.config.get(MASTER_CONFIG)
            if master is None:
                raise FeathubException(
                    "master url containing host and port has to be set with session "
                    "deployment mode."
                )

            parse_result = urlparse(master)
            if (
                not parse_result.scheme
                or not parse_result.hostname
                or not parse_result.port
            ):
                # Prepend schema
                parse_result = urlparse("//" + master)

            jobmanager_rpc_address = parse_result.hostname
            jobmanager_rpc_port = str(parse_result.port)
            if jobmanager_rpc_address is None or jobmanager_rpc_port is None:
                raise FeathubException(
                    "master url containing host and port has to be set with session "
                    "deployment mode."
                )
            t_env, class_loader = self._get_table_env(
                jobmanager_rpc_address, jobmanager_rpc_port
            )
            self.flink_table_builder = FlinkTableBuilder(
                t_env,
                class_loader,
                self.registry,
            )
            self.flink_job_submitter = FlinkSessionClusterJobSubmitter(self)
        else:
            raise FeathubException(
                f"Unsupported deployment mode {self.deployment_mode}."
            )

    def get_table(
        self,
        feature_descriptor: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> FlinkTable:
        feature_descriptor = self._resolve_table_descriptor(feature_descriptor)

        return FlinkTable(
            flink_processor=self,
            feature=feature_descriptor,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def materialize_features(
        self,
        materialization_descriptors: Sequence[MaterializationDescriptor],
    ) -> ProcessorJob:
        resolved_materialization_descriptor = []
        join_tables = {}
        for materialization_descriptor in materialization_descriptors:
            if (
                materialization_descriptor.ttl is not None
                or not materialization_descriptor.allow_overwrite
            ):
                raise RuntimeError("Unsupported operation.")

            resolved_feature_descriptor = self._resolve_table_descriptor(
                materialization_descriptor.feature_descriptor
            )
            resolved_materialization_descriptor.append(
                MaterializationDescriptor(
                    feature_descriptor=resolved_feature_descriptor,
                    sink=materialization_descriptor.sink,
                    ttl=materialization_descriptor.ttl,
                    start_datetime=materialization_descriptor.start_datetime,
                    end_datetime=materialization_descriptor.end_datetime,
                    allow_overwrite=materialization_descriptor.allow_overwrite,
                )
            )

            if self.registry.registry_type == LocalRegistry.REGISTRY_TYPE:
                # Get the tables to join in order to compute the feature if we are using
                # a local registry.
                join_tables.update(self._get_join_tables(resolved_feature_descriptor))

        return self.flink_job_submitter.submit(
            materialization_descriptors=resolved_materialization_descriptor,
            local_registry_tables=join_tables,
        )

    def _get_table_env(
        self,
        jobmanager_rpc_address: Optional[str] = None,
        jobmanager_rpc_port: Optional[str] = None,
    ) -> Tuple[StreamTableEnvironment, ClassLoader]:

        if jobmanager_rpc_address is not None and jobmanager_rpc_port is not None:
            # PyFlink 1.15 do not support initialize StreamExecutionEnvironment with
            # config. Currently, we work around by setting the environment. We
            # should initialize the StreamExecutionEnvironment with config after
            # PyFlink 1.16.
            os.environ.setdefault(
                "SUBMIT_ARGS",
                f"remote -m {jobmanager_rpc_address}:{jobmanager_rpc_port}",
            )

        class_loader = get_flink_context_class_loader()
        with class_loader:
            native_flink_config = Configuration()
            native_flink_config.set_string(
                "table.local-time-zone", self.config.get(TIMEZONE_CONFIG)
            )

            prefix_len = len(NATIVE_CONFIG_PREFIX)
            for k, v in self.config.original_props_with_prefix(
                NATIVE_CONFIG_PREFIX, False
            ).items():
                if (
                    k in NATIVE_CONFIG_PROCESSOR_CONFIG_MAP
                    and NATIVE_CONFIG_PROCESSOR_CONFIG_MAP[k]
                    in self.config.config_values
                    and v
                    != self.config.config_values[NATIVE_CONFIG_PROCESSOR_CONFIG_MAP[k]]
                ):
                    raise FeathubConfigurationException(
                        f"Native config: {k} is conflict with processor config: "
                        f"{NATIVE_CONFIG_PROCESSOR_CONFIG_MAP[k]}."
                    )
                native_flink_config.set_string(k[prefix_len:], v)

            env = StreamExecutionEnvironment.get_execution_environment()
            table_env = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.new_instance()
                .with_configuration(native_flink_config)
                .build(),
            )

            if jobmanager_rpc_address is not None and jobmanager_rpc_port is not None:
                os.environ.pop("SUBMIT_ARGS")

            return (
                table_env,
                get_flink_context_class_loader(),
            )

    def _resolve_table_descriptor(
        self, features: Union[str, TableDescriptor]
    ) -> TableDescriptor:
        if isinstance(features, str):
            features = self.registry.get_features(name=features)
        elif isinstance(features, FeatureView) and features.is_unresolved():
            features = self.registry.build_features([features])[0]

        return features

    def _get_join_tables(self, table: TableDescriptor) -> Dict[str, TableDescriptor]:
        """
        Get the tables to join with the given table.

        Recursively query the resolved table descriptor of the tables to be joined with
        the given table.
        """
        table = self._resolve_table_descriptor(table)

        if not isinstance(table, FeatureView) or isinstance(table, SqlFeatureView):
            return {}

        join_table = self._get_join_tables(table.get_resolved_source())
        for feature in table.get_resolved_features():
            if isinstance(feature.transform, JoinTransform):
                join_table_name = feature.transform.table_name
                join_table_descriptor = self.registry.get_features(join_table_name)
                join_table = {
                    **join_table,
                    **self._get_join_tables(join_table_descriptor),
                    join_table_name: join_table_descriptor,
                }
        return join_table

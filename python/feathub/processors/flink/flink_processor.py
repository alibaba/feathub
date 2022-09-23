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
import os
from datetime import timedelta, datetime
from typing import Optional, List, Union, Dict

import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
)

from feathub.common.exceptions import (
    FeathubException,
)
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.online_stores.online_store import OnlineStore
from feathub.processors.flink.flink_deployment_mode import DeploymentMode
from feathub.processors.flink.flink_table import FlinkTable
from feathub.processors.flink.table_builder.flink_table_builder import (
    FlinkTableBuilder,
)
from feathub.processors.flink.job_submitter.flink_job_submitter import FlinkJobSubmitter
from feathub.processors.flink.job_submitter.flink_kubernetes_application_cluster_job_submitter import (  # noqa
    FlinkKubernetesApplicationClusterJobSubmitter,
)
from feathub.processors.flink.job_submitter.flink_session_cluster_job_submitter import (
    FlinkSessionClusterJobSubmitter,
)
from feathub.processors.processor import Processor
from feathub.processors.processor_job import ProcessorJob
from feathub.registries.local_registry import LocalRegistry
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor

logger = logging.getLogger(__file__)


class FlinkProcessor(Processor):
    """
    The FlinkProcessor computes features with Flink.

    Basic Configurations:
        deployment_mode: The flink job deployment mode, it could be "cli", "session", or
                         "kubernetes-application". Default to "session".

    Session Mode Configuration:
        rest.address: The ip or hostname where the JobManager runs. Required.
        rest.port: The port where the JobManager runs. Required.

    Kubernetes Application Mode Configuration:
        flink_home: The path to the Flink distribution. If not specified, it uses the
                    Flink's distribution in PyFlink.
        kubernetes.image: The docker image to start the JobManager and TaskManager pod.
                          Default to "feathub:latest".
        kubernetes.namespace: The namespace of the Kubernetes cluster to run the Flink
                              job. Default to "default".
        kubernetes.config.file: The kubernetes config file is used to connector to
                                the Kubernetes cluster. Default to "~/.kube/config".
        flink.*: This can set and pass arbitrary Flink configuration. The "flink" prefix
                 in the key is removed before passing to Flink. For example, you can set
                 the default parallelism via "flink.parallelism.default".
                 configuration is overridden, e.g. "kubernetes.namespace" and
                 "kubernetes.config.file".
    """

    PROCESSOR_TYPE = "flink"

    def __init__(
        self, config: Dict, stores: Dict[str, OnlineStore], registry: Registry
    ) -> None:
        """
        Instantiate the FlinkProcessor.

        :param config: The processor configuration.
        :param stores: A dict that maps each store type to an online store.
        :param registry: An entity registry.
        """
        super().__init__()
        self.config = config
        self.stores = stores
        self.registry = registry

        try:
            self.deployment_mode = DeploymentMode(
                config.get("deployment_mode", "session")
            )
        except ValueError:
            raise FeathubException("Unsupported deployment mode.")

        if self.deployment_mode == DeploymentMode.CLI:
            self.flink_table_builder = FlinkTableBuilder(
                self._get_table_env(), self.registry
            )

            # The type of flink_job_submitter is set explicitly to the base class
            # FlinkJobSubmitter so that the type checking won't complain.
            self.flink_job_submitter: FlinkJobSubmitter = (
                FlinkSessionClusterJobSubmitter(self, self.stores)
            )
        elif self.deployment_mode == DeploymentMode.SESSION:
            jobmanager_rpc_address = config.get("rest.address")
            jobmanager_rpc_port = config.get("rest.port")
            if jobmanager_rpc_address is None or jobmanager_rpc_port is None:
                raise FeathubException(
                    "rest.address or rest.port has to be set with session "
                    "deployment mode."
                )
            self.flink_table_builder = FlinkTableBuilder(
                self._get_table_env(jobmanager_rpc_address, jobmanager_rpc_port),
                self.registry,
            )
            self.flink_job_submitter = FlinkSessionClusterJobSubmitter(
                self, self.stores
            )
        elif self.deployment_mode == DeploymentMode.KUBERNETES_APPLICATION:
            self.flink_table_builder = FlinkTableBuilder(
                self._get_table_env(), self.registry
            )
            self.flink_job_submitter = FlinkKubernetesApplicationClusterJobSubmitter(
                processor_config=config,
                registry_type=self.registry.registry_type,
                registry_config=self.registry.config,
            )
        else:
            raise FeathubException(
                f"Unsupported deployment mode {self.deployment_mode}."
            )

    def get_table(
        self,
        features: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> FlinkTable:
        features = self._resolve_table_descriptor(features)

        return FlinkTable(
            flink_processor=self,
            feature=features,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def materialize_features(
        self,
        features: Union[str, TableDescriptor],
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        if ttl is not None or not allow_overwrite:
            raise RuntimeError("Unsupported operation.")

        features = self._resolve_table_descriptor(features)

        # Get the tables to join in order to compute the feature if we are using a local
        # registry.
        join_tables = (
            self._get_join_tables(features)
            if self.registry.registry_type == LocalRegistry.REGISTRY_TYPE
            else {}
        )

        return self.flink_job_submitter.submit(
            features=features,
            keys=None,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            sink=sink,
            local_registry_tables=join_tables,
            allow_overwrite=allow_overwrite,
        )

    def get_online_features(
        self,
        table_name: str,
        input_data: pd.DataFrame,
        feature_fields: Optional[List[str]] = None,
        include_timestamp_field: bool = False,
        store_type: Optional[str] = None,
    ) -> pd.DataFrame:
        # TODO: to be implemented
        pass

    @staticmethod
    def _get_table_env(
        jobmanager_rpc_address: Optional[str] = None,
        jobmanager_rpc_port: Optional[str] = None,
    ) -> StreamTableEnvironment:
        if jobmanager_rpc_address is not None and jobmanager_rpc_port is not None:
            # PyFlink 1.15 do not support initialize StreamExecutionEnvironment with
            # config. Currently, we work around by setting the environment. We should
            # initialize the StreamExecutionEnvironment with config after PyFlink 1.16.
            os.environ.setdefault(
                "SUBMIT_ARGS",
                f"remote -m {jobmanager_rpc_address}:{jobmanager_rpc_port}",
            )

        env = StreamExecutionEnvironment.get_execution_environment()
        table_env = StreamTableEnvironment.create(env)

        if jobmanager_rpc_address is not None and jobmanager_rpc_port is not None:
            os.environ.pop("SUBMIT_ARGS")

        return table_env

    def _resolve_table_descriptor(
        self, features: Union[str, TableDescriptor]
    ) -> TableDescriptor:
        if isinstance(features, str):
            features = self.registry.get_features(name=features)
        elif isinstance(features, FeatureView) and features.is_unresolved():
            features = self.registry.get_features(name=features.name)

        return features

    def _get_join_tables(self, table: TableDescriptor) -> Dict[str, TableDescriptor]:
        """
        Get the tables to join with the given table.

        Recursively query the resolved table descriptor of the tables to be joined with
        the given table.
        """
        table = self._resolve_table_descriptor(table)

        if not isinstance(table, FeatureView):
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

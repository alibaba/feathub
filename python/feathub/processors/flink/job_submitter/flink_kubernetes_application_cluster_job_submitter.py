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
import base64
import logging
import os
import pickle
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, Future, Executor
from datetime import datetime
from subprocess import Popen
from typing import Dict, Optional, List, Union

import kubernetes.watch
import pandas as pd
from kubernetes.client import (
    CoreV1Api,
    AppsV1Api,
    V1ObjectMeta,
    V1ConfigMap,
    V1OwnerReference,
)
from kubernetes.config import load_kube_config
from pyflink.find_flink_home import _find_flink_home  # noqa

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.flink.flink_processor_config import (
    FlinkProcessorConfig,
    KUBERNETES_IMAGE_CONFIG,
    KUBERNETES_NAMESPACE_CONFIG,
    KUBERNETES_CONFIG_FILE_CONFIG,
    NATIVE_CONFIG_PREFIX,
    FLINK_HOME_CONFIG,
)
from feathub.processors.flink.job_submitter.feathub_job_descriptor import (
    FeathubJobDescriptor,
)
from feathub.processors.flink.job_submitter.flink_job_submitter import (
    FlinkJobSubmitter,
)
from feathub.processors.processor_job import ProcessorJob
from feathub.table.table_descriptor import TableDescriptor

logger = logging.getLogger(__file__)


class FlinkKubernetesApplicationClusterJobSubmitter(FlinkJobSubmitter):
    """The Flink job submitter for kubernetes application cluster."""

    def __init__(
        self,
        processor_config: FlinkProcessorConfig,
        registry_type: str,
        registry_config: Dict,
    ) -> None:
        """
        Instantiate the FlinkKubernetesApplicationClusterJobSubmitter.

        :param processor_config: The Flink processor configuration.
        :param registry_type: The type of the registry.
        :param registry_config: The registry configuration.
        """
        self.processor_config = processor_config
        self.registry_type = registry_type
        self.registry_config = registry_config
        flink_home = processor_config.get(FLINK_HOME_CONFIG)
        if flink_home is None:
            flink_home = _find_flink_home()
            logger.info(
                f"flink_home is not configured, using flink home from "
                f"pyflink {flink_home}."
            )
        self._flink_cli_executable = os.path.join(flink_home, "bin", "flink")
        self._executor = ThreadPoolExecutor()

        self.flink_kubernetes_image = processor_config.get(KUBERNETES_IMAGE_CONFIG)
        self.kube_namespace = processor_config.get(KUBERNETES_NAMESPACE_CONFIG)
        self.kube_config_file = processor_config.get(KUBERNETES_CONFIG_FILE_CONFIG)

        load_kube_config(self.kube_config_file)
        self.kube_core_v1_api = CoreV1Api()
        self.kube_apps_v1_api = AppsV1Api()

    def submit(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
        sink: FeatureTable,
        local_registry_tables: Dict[str, TableDescriptor],
        allow_overwrite: bool,
    ) -> "FlinkApplicationClusterJob":
        job_descriptor = FeathubJobDescriptor(
            features=features,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            sink=sink,
            local_registry_tables=local_registry_tables,
            allow_overwrite=allow_overwrite,
            props=self.processor_config.original_props,
        )

        job_id = str(uuid.uuid4())
        self._create_job_configmap(configmap_name=job_id, job_descriptor=job_descriptor)
        self._submit_flink_job(job_id)

        self._patch_config_map_with_owner_reference_to_deployment(
            configmap_name=job_id, deployment_name=self._get_cluster_id(job_id)
        )

        future = self._executor.submit(
            self._watch_job_deployment_until_finished, job_id=job_id
        )
        return FlinkApplicationClusterJob(
            future,
            self._get_cluster_id(job_id),
            self.kube_namespace,
            self.kube_apps_v1_api,
            self._executor,
        )

    def _watch_job_deployment_until_finished(self, job_id: str) -> None:
        watch = kubernetes.watch.Watch()
        for event in watch.stream(
            func=self.kube_apps_v1_api.list_namespaced_deployment,
            field_selector=f"metadata.name={self._get_cluster_id(job_id)}",
            namespace=self.kube_namespace,
        ):
            if event["type"] == "DELETED":
                break

    def _create_job_configmap(
        self, configmap_name: str, job_descriptor: FeathubJobDescriptor
    ) -> None:
        metadata = V1ObjectMeta(
            name=configmap_name,
            deletion_grace_period_seconds=30,
            namespace=self.kube_namespace,
        )

        config_map = V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            binary_data={
                "feathub_job_descriptor": base64.encodebytes(
                    pickle.dumps(job_descriptor)
                ).decode()
            },
            metadata=metadata,
        )

        self.kube_core_v1_api.create_namespaced_config_map(
            namespace=self.kube_namespace, body=config_map
        )

    def _submit_flink_job(self, job_id: str) -> None:
        args = [
            self._flink_cli_executable,
            "run-application",
            "--target",
            "kubernetes-application",
            *self._get_flink_submit_configuration(),
            f"-Dkubernetes.pod-template-file={self._get_pod_template_path(job_id)}",
            f"-Dkubernetes.cluster-id={self._get_cluster_id(job_id)}",
            "-py",
            "/opt/flink/flink_application_cluster_job_entry.py",
            "/opt/flink/feathub_job_descriptor",
        ]
        p = Popen(args=args)
        p.wait()
        if p.returncode != 0:
            raise FeathubException("Fail to submit application job to kubernetes.")

    @staticmethod
    def _get_pod_template_path(job_id: str) -> str:
        template_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "pod-template.yaml")
        )
        with open(template_path) as f:
            template = f.read().format(configmap_name=job_id)

        path = tempfile.NamedTemporaryFile().name
        with open(path, "w") as f:
            f.write(template)
        return path

    @staticmethod
    def _get_cluster_id(job_id: str) -> str:
        return f"job-{job_id}"

    def _patch_config_map_with_owner_reference_to_deployment(
        self, configmap_name: str, deployment_name: str
    ) -> None:
        deployment = self.kube_apps_v1_api.read_namespaced_deployment(
            deployment_name, self.kube_namespace
        )

        if deployment is None:
            raise FeathubException(
                "Failed to find deployment with name {}.".format(deployment_name)
            )
        deployment_uid = deployment.metadata.uid

        owner_reference = V1OwnerReference(
            api_version="apps/v1",
            block_owner_deletion=True,
            controller=True,
            kind="Deployment",
            name=deployment_name,
            uid=deployment_uid,
        )
        config_map = V1ConfigMap(
            metadata=V1ObjectMeta(owner_references=[owner_reference])
        )

        self.kube_core_v1_api.patch_namespaced_config_map(
            name=configmap_name, namespace=self.kube_namespace, body=config_map
        )

    def _get_flink_submit_configuration(self) -> List[str]:
        flink_config = {}
        for k, v in self.processor_config.original_props_with_prefix(
            NATIVE_CONFIG_PREFIX, True
        ).items():
            flink_config[k] = v

        flink_config["kubernetes.container.image"] = self.flink_kubernetes_image
        flink_config["kubernetes.namespace"] = self.kube_namespace
        if self.kube_config_file is not None:
            flink_config["kubernetes.config.file"] = self.kube_config_file

        return [f"-D{k}={v}" for k, v in flink_config.items()]


class FlinkApplicationClusterJob(ProcessorJob):
    """Represent a Flink job that runs in Application cluster."""

    def __init__(
        self,
        job_future: Future,
        cluster_id: str,
        kube_namespace: str,
        kube_apps_v1_api: AppsV1Api,
        executor: Executor,
    ):
        """
        Instantiate a FlinkApplicationClusterJob.

        :param job_future: A Future object which is done when the application Flink job
                           reaches global termination state.
        :param cluster_id: The cluster id of the Flink application cluster where the
                           Flink job runs.
        :param kube_namespace: The namespace of the Kubernetes cluster where the
                               Flink application cluster runs.
        :param kube_apps_v1_api: The Kubernetes api object.
        :param executor: The executor to run async task.
        """
        super().__init__()
        self._job_future = job_future
        self._cluster_id = cluster_id
        self._kube_namespace = kube_namespace
        self._kube_apps_v1_api = kube_apps_v1_api
        self._executor = executor

    def cancel(self) -> Future:
        return self._executor.submit(self._cancel)

    def _cancel(self) -> None:
        self._kube_apps_v1_api.delete_namespaced_deployment(
            name=self._cluster_id, namespace=self._kube_namespace, async_req=True
        )

    def wait(self, timeout_ms: Optional[int] = None) -> None:
        timeout_sec = None if timeout_ms is None else timeout_ms / 1000
        self._job_future.result(timeout=timeout_sec)

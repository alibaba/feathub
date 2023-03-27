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
import base64
import pickle
import unittest
from unittest.mock import patch, MagicMock

import feathub.processors.flink.job_submitter.flink_kubernetes_application_cluster_job_submitter  # noqa
from kubernetes.client import CoreV1Api, AppsV1Api

from feathub.processors.flink.flink_processor_config import FlinkProcessorConfig
from feathub.processors.flink.job_submitter import (
    flink_kubernetes_application_cluster_job_submitter,
)
from feathub.processors.flink.job_submitter.feathub_job_descriptor import (
    FeathubJobDescriptor,
)
from feathub.processors.flink.job_submitter.flink_kubernetes_application_cluster_job_submitter import (  # noqa
    FlinkKubernetesApplicationClusterJobSubmitter,
)
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.table.schema import Schema


class FlinkKubernetesApplicationClusterJobSubmitterTest(unittest.TestCase):
    def setUp(self) -> None:
        processor_config = {
            "processor.flink.deployment_mode": "kubernetes-application",
            "processor.flink.flink_home": "/flink-home",
        }

        feathub.processors.flink.job_submitter.flink_kubernetes_application_cluster_job_submitter.load_kube_config = (  # noqa
            MagicMock()
        )
        self.submitter = FlinkKubernetesApplicationClusterJobSubmitter(
            FlinkProcessorConfig(processor_config),
            registry_type="local",
            registry_config={"namespace": "default"},
        )
        self.submitter.kube_core_v1_api = MagicMock(spec=CoreV1Api)
        self.submitter.kube_apps_v1_api = MagicMock(spec=AppsV1Api)

    def test_init(self):
        submitter = FlinkKubernetesApplicationClusterJobSubmitter(
            FlinkProcessorConfig({"processor.flink.flink_home": "/tmp"}), "local", {}
        )
        self.assertEqual("/tmp/bin/flink", submitter._flink_cli_executable)
        self.assertEqual("feathub:latest", submitter.flink_kubernetes_image)
        self.assertEqual("default", submitter.kube_namespace)

        submitter = FlinkKubernetesApplicationClusterJobSubmitter(
            FlinkProcessorConfig(
                {
                    "processor.flink.flink_home": "/tmp",
                    "processor.flink.kubernetes.image": "feathub:0.1",
                    "processor.flink.kubernetes.namespace": "test-namespace",
                }
            ),
            "local",
            {},
        )
        self.assertEqual("/tmp/bin/flink", submitter._flink_cli_executable)
        self.assertEqual("feathub:0.1", submitter.flink_kubernetes_image)
        self.assertEqual("test-namespace", submitter.kube_namespace)

    def test_create_configmap(self):
        source = FileSystemSource("source", "/dummy/path", "csv", Schema([], []))
        sink = FileSystemSink("/dummy/path", "csv")

        mock_popen_return = MagicMock()
        mock_popen_return.returncode = 0

        deployment = MagicMock()
        deployment.metadata = MagicMock()
        deployment.metadata.uid = "1234"

        apps_v1_api = self.submitter.kube_apps_v1_api
        apps_v1_api.read_namespaced_deployment.return_value = deployment

        with patch.object(
            flink_kubernetes_application_cluster_job_submitter,
            "Popen",
            return_value=mock_popen_return,
        ) as mock_method:
            self.submitter.submit(source, None, None, None, sink, {}, True)
            core_v1_api = self.submitter.kube_core_v1_api
            core_v1_api.create_namespaced_config_map.assert_called_once()
            create_config_map_call_args = (
                core_v1_api.create_namespaced_config_map.call_args
            )
            binary_data = create_config_map_call_args[1]["body"].binary_data
            expected_job_descriptor = FeathubJobDescriptor(
                source,
                None,
                None,
                None,
                sink,
                {},
                True,
                self.submitter.processor_config.original_props,
            )
            self.assertEqual(
                expected_job_descriptor,
                pickle.loads(
                    base64.decodebytes(binary_data["feathub_job_descriptor"].encode())
                ),
            )

            mock_method.assert_called_once()
            popen_call_args = mock_method.call_args
            args = popen_call_args[1]["args"]
            expected_args = [
                "/flink-home/bin/flink",
                "run-application",
                "--target",
                "kubernetes-application",
                "-Dkubernetes.container.image=feathub:latest",
                "-Dkubernetes.namespace=default",
                "-py",
                "/opt/flink/flink_application_cluster_job_entry.py",
                "/opt/flink/feathub_job_descriptor",
            ]
            for arg in expected_args:
                self.assertTrue(arg in args, f"{arg} not in flink cli args {args}")

            apps_v1_api.read_namespaced_deployment.assert_called_once()
            core_v1_api.patch_namespaced_config_map.assert_called_once()
            patch_config_map_call_args = (
                core_v1_api.patch_namespaced_config_map.call_args
            )
            self.assertEqual(
                "1234",
                patch_config_map_call_args[1]["body"].metadata.owner_references[0].uid,
            )

    def test_submit_job_with_additional_flink_configuration(self):
        source = FileSystemSource("source", "/dummy/path", "csv", Schema([], []))
        sink = FileSystemSink("/dummy/path", "csv")
        self.submitter.processor_config.original_props[
            "processor.flink.native.additional.configuration"
        ] = "value"

        mock_popen_return = MagicMock()
        mock_popen_return.returncode = 0
        with patch.object(
            flink_kubernetes_application_cluster_job_submitter,
            "Popen",
            return_value=mock_popen_return,
        ) as mock_method:
            self.submitter.submit(source, None, None, None, sink, {}, True)
            mock_method.assert_called_once()
            popen_call_args = mock_method.call_args
            args = popen_call_args[1]["args"]

            expected_arg = "-Dadditional.configuration=value"
            self.assertTrue(
                expected_arg in args, f"{expected_arg} not in flink cli args {args}"
            )

    def test_submit_job_with_join_table(self):
        source = FileSystemSource("source", "/dummy/path", "csv", Schema([], []))
        table = FileSystemSource("table", "/dummy/path", "csv", Schema([], []))
        sink = FileSystemSink("/dummy/path", "csv")
        self.submitter.processor_config.original_props[
            "processor.flink.native.additional.configuration"
        ] = "value"

        mock_popen_return = MagicMock()
        mock_popen_return.returncode = 0
        with patch.object(
            flink_kubernetes_application_cluster_job_submitter,
            "Popen",
            return_value=mock_popen_return,
        ):
            self.submitter.submit(
                source, None, None, None, sink, {"table": table}, True
            )
            core_v1_api = self.submitter.kube_core_v1_api
            core_v1_api.create_namespaced_config_map.assert_called_once()
            create_config_map_call_args = (
                core_v1_api.create_namespaced_config_map.call_args
            )
            binary_data = create_config_map_call_args[1]["body"].binary_data
            expected_job_descriptor = FeathubJobDescriptor(
                source,
                None,
                None,
                None,
                sink,
                {"table": table},
                True,
                self.submitter.processor_config.original_props,
            )
            self.assertEqual(
                expected_job_descriptor,
                pickle.loads(
                    base64.decodebytes(binary_data["feathub_job_descriptor"].encode())
                ),
            )

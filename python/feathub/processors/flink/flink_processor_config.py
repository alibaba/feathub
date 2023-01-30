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

from typing import Dict, Any, List

from feathub.common.config import ConfigDef
from feathub.common.validators import in_list
from feathub.processors.flink.flink_deployment_mode import DeploymentMode
from feathub.processors.processor_config import ProcessorConfig, PROCESSOR_PREFIX

FLINK_PROCESSOR_PREFIX = PROCESSOR_PREFIX + "flink."

DEPLOYMENT_MODE_CONFIG = FLINK_PROCESSOR_PREFIX + "deployment_mode"
DEPLOYMENT_MODE_DOC = "The flink job deployment mode."

REST_ADDRESS_CONFIG = FLINK_PROCESSOR_PREFIX + "rest.address"
REST_ADDRESS_DOC = "The ip or hostname where the JobManager runs."

REST_PORT_CONFIG = FLINK_PROCESSOR_PREFIX + "rest.port"
REST_PORT_DOC = "The port where the JobManager runs."

FLINK_HOME_CONFIG = FLINK_PROCESSOR_PREFIX + "flink_home"
FLINK_HOME_DOC = (
    "The path to the Flink distribution. If not specified, it uses the "
    "Flink's distribution in PyFlink."
)

KUBERNETES_IMAGE_CONFIG = FLINK_PROCESSOR_PREFIX + "kubernetes.image"
KUBERNETES_IMAGE_DOC = "The docker image to start the JobManager and TaskManager pod."

KUBERNETES_NAMESPACE_CONFIG = FLINK_PROCESSOR_PREFIX + "kubernetes.namespace"
KUBERNETES_NAMESPACE_DOC = (
    "The namespace of the Kubernetes cluster to run the Flink job."
)

KUBERNETES_CONFIG_FILE_CONFIG = FLINK_PROCESSOR_PREFIX + "kubernetes.config.file"
KUBERNETES_CONFIG_FILE_DOC = (
    "The kubernetes config file is used to connector to the Kubernetes " "cluster."
)

NATIVE_CONFIG_PREFIX = FLINK_PROCESSOR_PREFIX + "native."

flink_processor_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=DEPLOYMENT_MODE_CONFIG,
        value_type=str,
        description=DEPLOYMENT_MODE_DOC,
        default_value="session",
        validator=in_list(*[t.value for t in DeploymentMode]),
    ),
    ConfigDef(
        name=REST_ADDRESS_CONFIG,
        value_type=str,
        description=REST_ADDRESS_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=REST_PORT_CONFIG,
        value_type=int,
        description=REST_PORT_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=FLINK_HOME_CONFIG,
        value_type=str,
        description=FLINK_HOME_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=KUBERNETES_IMAGE_CONFIG,
        value_type=str,
        description=KUBERNETES_IMAGE_DOC,
        default_value="feathub:latest",
    ),
    ConfigDef(
        name=KUBERNETES_NAMESPACE_CONFIG,
        value_type=str,
        description=KUBERNETES_NAMESPACE_DOC,
        default_value="default",
    ),
    ConfigDef(
        name=KUBERNETES_CONFIG_FILE_CONFIG,
        value_type=str,
        description=KUBERNETES_CONFIG_FILE_DOC,
        default_value="~/.kube/config",
    ),
]


class FlinkProcessorConfig(ProcessorConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(flink_processor_config_defs)

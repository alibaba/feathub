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
from enum import Enum
from typing import Any, Dict, List

from feathub.common.config import BaseConfig, ConfigDef
from feathub.common.validators import in_list, gt


class MetricStoreType(Enum):
    PROMETHEUS = "prometheus"


METRIC_STORE_PREFIX = "metric_store."

METRIC_STORE_TYPE_CONFIG = METRIC_STORE_PREFIX + "type"
METRIC_STORE_TYPE_DOC = "The type of the metric store to use."

METRIC_STORE_REPORT_INTERVAL_SEC_CONFIG = METRIC_STORE_PREFIX + "report_interval_sec"
METRIC_STORE_REPORT_INTERVAL_SEC_DOC = "The interval in seconds to report metrics."

METRIC_STORE_NAMESPACE_CONFIG = METRIC_STORE_PREFIX + "namespace"
METRIC_STORE_NAMESPACE_DOC = """
The namespace to report metrics to the metric store. Metrics within
different namespace will not overwrite each other.
"""


metric_store_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=METRIC_STORE_TYPE_CONFIG,
        value_type=str,
        description=METRIC_STORE_TYPE_DOC,
        default_value=None,
        validator=in_list(None, *[t.value for t in MetricStoreType]),
    ),
    ConfigDef(
        name=METRIC_STORE_REPORT_INTERVAL_SEC_CONFIG,
        value_type=int,
        description=METRIC_STORE_REPORT_INTERVAL_SEC_DOC,
        default_value=None,
        validator=gt(0),  # type: ignore
    ),
    ConfigDef(
        name=METRIC_STORE_NAMESPACE_CONFIG,
        value_type=str,
        description=METRIC_STORE_NAMESPACE_DOC,
        default_value="default",
    ),
]


class MetricStoreConfig(BaseConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(metric_store_config_defs)

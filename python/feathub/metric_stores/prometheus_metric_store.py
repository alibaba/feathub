#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
from datetime import timedelta
from typing import Any
from typing import Dict, List, OrderedDict

from feathub.common.config import ConfigDef
from feathub.feature_tables.sinks.prometheus_sink import PrometheusSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.feature import Feature
from feathub.metric_stores.metric import Metric
from feathub.metric_stores.metric_store import MetricStore
from feathub.metric_stores.metric_store_config import (
    METRIC_STORE_PREFIX,
)
from feathub.metric_stores.metric_store_config import (
    MetricStoreConfig,
)

PROMETHEUS_METRIC_STORE_PREFIX = METRIC_STORE_PREFIX + "prometheus."

SERVER_URL_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "server_url"
SERVER_URL_DOC = "The PushGateway server URL including scheme, host name, and port."

DELETE_ON_SHUTDOWN_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "delete_on_shutdown"
DELETE_ON_SHUTDOWN_DOC = """
Whether to delete metrics from Prometheus when the job finishes. When set to true,
Feathub will try its best to delete the metrics but this is not guaranteed.
"""


prometheus_metric_store_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=SERVER_URL_CONFIG,
        value_type=str,
        description=SERVER_URL_DOC,
    ),
    ConfigDef(
        name=DELETE_ON_SHUTDOWN_CONFIG,
        value_type=bool,
        description=DELETE_ON_SHUTDOWN_DOC,
        default_value=True,
    ),
]


class PrometheusMetricStoreConfig(MetricStoreConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(prometheus_metric_store_config_defs)


# TODO: Allow users to configure feature name pattern.
class PrometheusMetricStore(MetricStore):
    """
    A metric store reporting metrics to Prometheus through push gateway. For
    user-defined Metrics, they would be reported in the following format:

    - metric name: "{namespace}_{feature_name}_{metric_type}".
    - metric labels:
        - job: The namespace of the metric store.
        - table_name: The name of the sink where the host features would be written to.
        - feature_name: The name of the host feature.
        - other metric-specific labels exposed through Metric#get_tags.
    """

    def __init__(self, props: Dict) -> None:
        """
        :param props: The metric store properties.
        """
        super().__init__(props)
        prometheus_metric_store_config = PrometheusMetricStoreConfig(props)
        self.server_url = prometheus_metric_store_config.get(SERVER_URL_CONFIG)
        self.delete_on_shutdown = prometheus_metric_store_config.get(
            DELETE_ON_SHUTDOWN_CONFIG
        )

    def _get_metric_tags(
        self, metric: Metric, feature: Feature, data_sink: Sink
    ) -> OrderedDict[str, str]:
        # Prometheus sink supports configuring the common labels of metrics
        # with extraLabels, so here only metric/feature-specific labels need
        # to be returned.
        tags = collections.OrderedDict(
            [
                ("feature_name", feature.name),
            ]
        )
        for key, value in metric.get_tags().items():
            tags[key] = value
        return tags

    def _get_metrics_sink(self, data_sink: Sink) -> Sink:
        return PrometheusSink(
            server_url=self.server_url,
            job_name=self.namespace,
            delete_on_shutdown=self.delete_on_shutdown,
            extra_labels={"table_name": data_sink.name},
            retry_timeout=timedelta(seconds=self.report_interval_sec),
        )

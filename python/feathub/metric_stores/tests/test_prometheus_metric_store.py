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
import re
from abc import ABC
from datetime import timedelta
from typing import List
from urllib import request

from prometheus_client import (
    CollectorRegistry,
    Gauge,
    push_to_gateway,
    delete_from_gateway,
)
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
)
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.metric_stores.metric import Count, Ratio, Metric
from feathub.tests.feathub_it_test_base import FeathubITTestBase


# TODO: replace this method with simple `text[len(prefix):]` after fixing the
#  format configuration gap between black and flake8
def remove_prefix(text, prefix):
    len_prefix = len(prefix)
    return text[len_prefix:]


class PrometheusPushGatewayContainer(DockerContainer):
    def __init__(self, image="prom/pushgateway:v1.6.0", **kwargs):
        super(PrometheusPushGatewayContainer, self).__init__(image, **kwargs)
        self.with_exposed_ports(9091)

    @wait_container_is_ready(IOError)
    def _wait_container_ready(self):
        registry = CollectorRegistry()
        g = Gauge("probe", "probe", registry=registry)
        g.set_to_current_time()
        push_to_gateway(self.get_server_url(), job="probe-job", registry=registry)
        delete_from_gateway(self.get_server_url(), "probe-job")

    def start(self):
        super().start()
        self._wait_container_ready()
        return self

    def get_server_url(self):
        return f"{self.get_container_host_ip()}:{self.get_exposed_port(9091)}"


class PrometheusMetricStoreITTest(ABC, FeathubITTestBase):
    prometheus_push_gateway_container: PrometheusPushGatewayContainer

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.prometheus_push_gateway_container = PrometheusPushGatewayContainer()
        cls.prometheus_push_gateway_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.prometheus_push_gateway_container.stop()

    def tearDown(self) -> None:
        super(PrometheusMetricStoreITTest, self).tearDown()
        delete_from_gateway(
            gateway=self.prometheus_push_gateway_container.get_server_url(),
            job="default",
            grouping_key={"table_name": ""},
        )

    def test_prometheus_metric_store(self):
        self._test_prometheus_metric_store(
            [
                Count(
                    window_size=timedelta(days=1),
                ),
            ]
        )

    def test_prometheus_metric_store_with_same_metric_name(self):
        self._test_prometheus_metric_store(
            [
                Count(
                    window_size=timedelta(days=1),
                ),
                Count(
                    filter_expr="> 0",
                    window_size=timedelta(days=1),
                ),
            ]
        )

    def test_prometheus_metric_store_with_different_window_size(self):
        self._test_prometheus_metric_store(
            [
                Count(
                    filter_expr="> 0",
                    window_size=timedelta(days=1),
                ),
                Ratio(
                    filter_expr="> 0",
                    window_size=timedelta(days=2),
                ),
            ]
        )

    def test_prometheus_metric_store_escape_characters(self):
        self._test_prometheus_metric_store(
            [
                Count(
                    filter_expr="> 0 AND name <> 'a,b'",
                    window_size=timedelta(days=1),
                ),
            ]
        )

    def _test_prometheus_metric_store(self, metric_definitions: List[Metric]):
        self.server_url = self.prometheus_push_gateway_container.get_server_url()
        self.client = self.get_client(
            extra_config={
                "metric_store": {
                    "type": "prometheus",
                    "report_interval_sec": 5,
                    "prometheus": {
                        "server_url": self.server_url,
                        "delete_on_shutdown": False,
                    },
                }
            }
        )
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                group_by_keys=["name"],
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
            ),
            metrics=metric_definitions,
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
        )

        self.client.materialize_features(
            features, sink=BlackHoleSink(), allow_overwrite=True
        ).wait()

        url = f"http://{self.server_url}/metrics"
        contents: str = request.urlopen(url).read().decode()
        metrics = []
        for content in contents.split("\n"):
            if content.startswith("#") or "default_total_cost" not in content:
                continue
            metric_value = content.split(" ")[-1]
            name_and_tags = content[: -len(metric_value) - 1]
            metric_name = name_and_tags.split("{")[0]
            tags_dict = dict()
            for tag_str in re.split(
                r'(?<="),', remove_prefix(name_and_tags, metric_name).strip("{}")
            ):
                tag_key = tag_str.split("=")[0]
                tag_value = remove_prefix(tag_str, tag_key)[2:-1]
                tags_dict[tag_key] = tag_value
            metrics.append((metric_name, tags_dict, metric_value))

        self.assertEqual(len(metrics), len(metric_definitions))
        for i in range(len(metrics)):
            self.assertEqual(
                metrics[i][0], f"default_total_cost_{metric_definitions[i].metric_type}"
            )
            self.assertEqual(metrics[i][1]["feature_name"], "total_cost")
            self.assertEqual(
                metrics[i][1]["metric_type"], metric_definitions[i].metric_type
            )
            self.assertEqual(
                metrics[i][1]["filter_expr"],
                ""
                if metric_definitions[i].filter_expr is None  # type: ignore
                else metric_definitions[i].filter_expr,  # type: ignore
            )
            self.assertEqual(metrics[i][1]["table_name"], "")
            self.assertEqual(
                metrics[i][1]["window_time_sec"],
                str(int(metric_definitions[i].window_size / timedelta(seconds=1))),
            )
            self.assertEqual(metrics[i][1]["job"], "default")
            # TODO: setup Prometheus server together with Prometheus PushGateway
            #  and verify history metric value.
            self.assertEqual(metrics[i][2], "0")

    def test_prometheus_metric_store_zero_window_size(self):
        with self.assertRaises(FeathubException) as cm:
            Count(window_size=timedelta(seconds=0))
        self.assertIn(
            "Metric window size 0:00:00 must be a positive value.", cm.exception.args[0]
        )

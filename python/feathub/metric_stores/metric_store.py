# Copyright 2022 The FeatHub Authors
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
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Dict, List, Optional, OrderedDict, Tuple, Sequence

from feathub.common.utils import generate_random_name
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
from feathub.metric_stores.metric import Metric
from feathub.metric_stores.metric_store_config import (
    MetricStoreConfig,
)
from feathub.metric_stores.metric_store_config import (
    MetricStoreType,
    METRIC_STORE_TYPE_CONFIG,
    METRIC_STORE_NAMESPACE_CONFIG,
    METRIC_STORE_REPORT_INTERVAL_SEC_CONFIG,
)
from feathub.processors.materialization_descriptor import MaterializationDescriptor
from feathub.table.table_descriptor import TableDescriptor


class MetricStore(ABC):
    """
    A MetricStore provides properties to set the metrics of a Feathub job
    into an external metric service.
    """

    def __init__(self, config: Dict) -> None:
        """
        :param config: The metric store configuration.
        """
        self._metric_store_config = MetricStoreConfig(config)

    @staticmethod
    def instantiate(props: Dict) -> Optional["MetricStore"]:
        """
        Instantiates a metric store using the given properties.
        """

        # TODO: remove code below after local metric store is introduced as
        #  default value.
        if METRIC_STORE_TYPE_CONFIG not in props:
            return None

        metric_store_config = MetricStoreConfig(props)
        metric_store_type = MetricStoreType(
            metric_store_config.get(METRIC_STORE_TYPE_CONFIG)
        )

        if metric_store_type == MetricStoreType.PROMETHEUS:
            from feathub.metric_stores.prometheus_metric_store import (
                PrometheusMetricStore,
            )

            return PrometheusMetricStore(props=props)

        raise RuntimeError(f"Failed to instantiate metric store with props={props}.")

    @property
    def namespace(self) -> str:
        return self._metric_store_config.get(METRIC_STORE_NAMESPACE_CONFIG)

    @property
    def report_interval(self) -> timedelta:
        return timedelta(
            seconds=self._metric_store_config.get(
                METRIC_STORE_REPORT_INTERVAL_SEC_CONFIG
            )
        )

    def create_metric_materialization_descriptors(
        self,
        feature_descriptor: TableDescriptor,
        data_sink: Sink,
    ) -> Sequence[MaterializationDescriptor]:
        """
        Creates materialization descriptors used to materialize metrics in the features
        of a table descriptor to this metric store.

        :param feature_descriptor: The descriptor that might contain metrics to be
                                   materialized.
        :param data_sink: The sink where the feature values will be written to.
        """
        descriptors = []
        window_sizes = set()
        for feature in feature_descriptor.get_output_features():
            for metric in feature.metrics:
                window_sizes.add(metric.window_size)
        for window_size in window_sizes:
            descriptors.append(
                MaterializationDescriptor(
                    feature_descriptor=self._get_metrics_view(
                        feature_descriptor, data_sink, window_size
                    ),
                    sink=self._get_metrics_sink(data_sink),
                    allow_overwrite=True,
                )
            )
        return descriptors

    def _get_metric_name(self, metric: Metric, feature: Feature) -> str:
        return f"{self.namespace}_{feature.name}_{metric.metric_type}"

    def _get_metric_tags(
        self, metric: Metric, feature: Feature, data_sink: Sink
    ) -> OrderedDict[str, str]:
        tags = collections.OrderedDict(
            [
                ("table_name", data_sink.name),
                ("feature_name", feature.name),
            ]
        )
        for key, value in metric.get_tags().items():
            tags[key] = value
        return tags

    # TODO: support treating zero window as infinite window in SlidingWindowTransform
    def _get_metrics_view(
        self, features_desc: TableDescriptor, data_sink: Sink, window_size: timedelta
    ) -> TableDescriptor:
        metric_name_count_dict: Dict[str, int] = dict()
        for feature in features_desc.get_output_features():
            for metric in feature.metrics:
                if window_size != metric.window_size:
                    continue
                metric_name = self._get_metric_name(metric, feature)
                metric_name_count_dict[metric_name] = (
                    metric_name_count_dict.get(metric_name, 0) + 1
                )

        # A dict whose keys are the metric names whose count is larger than 1,
        # values are list of tuples of temporary metric names and tags.
        metric_name_mapping_dict: Dict[str, List[Tuple[str, str]]] = dict()
        for metric_name, count in metric_name_count_dict.items():
            if count > 1:
                metric_name_mapping_dict[metric_name] = []

        metric_features = []
        for feature in features_desc.get_output_features():
            for metric in feature.metrics:
                if window_size != metric.window_size:
                    continue
                last_feature_name = feature.name
                for transform_function in metric.get_transform_functions():
                    tmp_feature_name = generate_random_name("feathub_metric")
                    transform = transform_function(last_feature_name)
                    metric_features.append(
                        Feature(
                            name=tmp_feature_name,
                            transform=transform,
                        )
                    )
                    last_feature_name = tmp_feature_name
                metric_name = self._get_metric_name(metric, feature)
                metric_tags = self._get_metric_tags(metric, feature, data_sink)
                metric_tags_str = ",".join(
                    f"{k}={self._escape_tag_value_str(v)}"
                    for k, v in metric_tags.items()
                )
                if metric_name in metric_name_mapping_dict:
                    metric_name_mapping_dict[metric_name].append(
                        (last_feature_name, metric_tags_str)
                    )
                else:
                    metric_features.append(
                        Feature(
                            name=metric_name,
                            transform=f"`{last_feature_name}`",
                            description=metric_tags_str,
                        )
                    )

        for metric_name, tmp_names_and_tags in metric_name_mapping_dict.items():
            metric_features.append(
                Feature(
                    name=metric_name,
                    transform=f"ARRAY({','.join(x[0] for x in tmp_names_and_tags)})",
                    description=";".join(x[1] for x in tmp_names_and_tags),
                )
            )

        window_size_sec = int(window_size / timedelta(seconds=1))

        sliding_feature_view = SlidingFeatureView(
            name=f"{features_desc.name}_metrics_{window_size_sec}",
            source=features_desc,
            features=metric_features,
        )

        # TODO: Chaining a next sliding window to support report interval throws
        #  Exceptions. Figure out why and add support for report interval.

        return DerivedFeatureView(
            name=f"{features_desc.name}_filtered_metrics_{window_size_sec}",
            source=sliding_feature_view,
            features=[
                Feature(
                    name=metric_name,
                    transform=f"`{metric_name}`",
                    description=sliding_feature_view.get_feature(
                        metric_name
                    ).description,
                )
                for metric_name in metric_name_count_dict.keys()
            ],
            keep_source_fields=False,
        )

    def _escape_tag_value_str(self, tag_value: str) -> str:
        characters_to_escape = [",", ";", "="]
        for char in characters_to_escape:
            tag_value = tag_value.replace(char, f"\\{char}")
        return tag_value

    @abstractmethod
    def _get_metrics_sink(self, data_sink: Sink) -> Sink:
        pass

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
from abc import ABC
from datetime import timedelta

import numpy as np
import pandas as pd

from feathub.common.test_utils import to_epoch_millis
from feathub.common.types import String, Int64
from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.metric_stores.metric import CountMap, Count, Ratio, Average
from feathub.metric_stores.metric_store import MetricStore
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class DummyMetricStore(MetricStore):
    def __init__(self) -> None:
        super(DummyMetricStore, self).__init__({})

    def _get_metrics_sink(self, data_sink: Sink) -> Sink:
        raise Exception()


class MetricStoreITTest(ABC, FeathubITTestBase):
    def test_metric_transformation(self):
        metric_store = DummyMetricStore()

        df = pd.DataFrame(
            [["2022-01-01 08:01:00", "abc", 1], ["2022-01-01 08:02:00", "abc", -1]],
            columns=["time", "string_v", "int64_v"],
        )
        source = self.create_file_source(
            df,
            data_format="json",
            schema=Schema.new_builder()
            .column("time", String)
            .column("string_v", String)
            .column("int64_v", Int64)
            .build(),
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        features: FeatureView = DerivedFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="int64_v",
                    transform="int64_v",
                    metrics=[
                        Count(
                            filter_expr="> 0",
                            window_size=timedelta(days=1),
                        ),
                        Ratio(
                            filter_expr="> 0",
                            window_size=timedelta(days=1),
                        ),
                        Average(
                            filter_expr="> 0",
                            window_size=timedelta(days=1),
                        ),
                    ],
                ),
                Feature(
                    name="string_v",
                    transform="string_v",
                    metrics=[
                        CountMap(
                            window_size=timedelta(days=1),
                        ),
                    ],
                ),
            ],
        )

        features = metric_store._get_metrics_view(
            features_desc=features,
            data_sink=BlackHoleSink(),
            window_size=timedelta(days=1),
        )

        result_df = self.client.get_features(
            # Remove the PeriodicEmitLastValueOperator to avoid having
            # metric results filtered according to report interval
            feature_descriptor=features.get_resolved_source()
        ).to_pandas()

        expected_result_df = pd.DataFrame(
            [
                [
                    to_epoch_millis("2022-01-01 23:59:59.999"),
                    1,
                    0.5,
                    1.0,
                    {"abc": 2},
                ],
                [
                    to_epoch_millis("2022-01-02 23:59:59.999"),
                    0,
                    0.0,
                    np.nan,
                    None,
                ],
            ],
            columns=[
                "window_time",
                "default_int64_v_count",
                "default_int64_v_ratio",
                "default_int64_v_average",
                "default_string_v_count_map",
            ],
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def test_metric_transformation_with_infinite_window(self):
        metric_store = DummyMetricStore()

        df = pd.DataFrame(
            [
                ["2022-01-01 08:01:00", "abc", 1],
                ["2022-01-01 08:02:00", "def", 2],
            ],
            columns=["time", "string_v", "int64_v"],
        )
        source = self.create_file_source(
            df,
            data_format="json",
            schema=Schema.new_builder()
            .column("time", String)
            .column("string_v", String)
            .column("int64_v", Int64)
            .build(),
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        features: FeatureView = DerivedFeatureView(
            name="features",
            source=source,
            features=[
                Feature(
                    name="int64_v",
                    transform="int64_v",
                    metrics=[
                        Count(),
                        Ratio(filter_expr="> 0"),
                        Average(),
                    ],
                ),
                Feature(
                    name="string_v",
                    transform="string_v",
                    metrics=[
                        CountMap(),
                    ],
                ),
            ],
        )

        features = metric_store._get_metrics_view(
            features_desc=features,
            data_sink=BlackHoleSink(),
            window_size=timedelta(seconds=0),
        )

        result_df = self.client.get_features(
            # Remove the PeriodicEmitLastValueOperator to avoid having
            # metric results filtered according to report interval
            feature_descriptor=features.get_resolved_source()
        ).to_pandas()

        expected_result_df = pd.DataFrame(
            [
                [
                    to_epoch_millis("2022-01-01 08:01:00.000"),
                    1,
                    1.0,
                    1.0,
                    {"abc": 1},
                ],
                [
                    to_epoch_millis("2022-01-01 08:02:00.000"),
                    2,
                    1.0,
                    1.5,
                    {"abc": 1, "def": 1},
                ],
            ],
            columns=[
                "window_time",
                "default_int64_v_count",
                "default_int64_v_ratio",
                "default_int64_v_average",
                "default_string_v_count_map",
            ],
        )

        self.assertTrue(expected_result_df.equals(result_df))

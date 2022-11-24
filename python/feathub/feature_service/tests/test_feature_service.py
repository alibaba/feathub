# Copyright 2022 The Feathub Authors
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

import pandas as pd
from typing import List

from feathub.common import types
from feathub.common.config import flatten_dict
from feathub.feature_service.feature_service import FeatureService
from feathub.feature_views.feature import Feature
from feathub.common.test_utils import LocalProcessorTestCase
from feathub.feature_service.local_feature_service import LocalFeatureService
from feathub.feature_tables.sinks.online_store_sink import OnlineStoreSink
from feathub.feature_tables.sources.online_store_source import OnlineStoreSource
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView


class FeatureServiceTest(LocalProcessorTestCase):
    def setUp(self):
        super().setUp()
        self.feature_service = LocalFeatureService(
            props={}, stores=self.stores, registry=self.registry
        )
        input_data_1 = pd.DataFrame(
            [
                ["Emma", 200, "2022-01-02 08:01:00"],
                ["Jack", 500, "2022-01-03 08:02:00"],
                ["Alex", 600, "2022-01-03 08:03:00"],
            ],
            columns=["name", "cost", "time"],
        )

        self.online_source_1 = self._materialize_and_get_online_store_source(
            name="online_store_source_1",
            store_type=MemoryOnlineStore.STORE_TYPE,
            table_name="table_name_1",
            input_data=input_data_1,
            keys=["name"],
        )

        input_data_2 = pd.DataFrame(
            [
                ["Emma", 250, "2022-01-02 08:04:00"],
                ["Jack", 500, "2022-01-03 08:05:00"],
                ["Alex", 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "distance", "time"],
        )
        self.online_source_2 = self._materialize_and_get_online_store_source(
            name="online_store_source_2",
            store_type=MemoryOnlineStore.STORE_TYPE,
            table_name="table_name_2",
            input_data=input_data_2,
            keys=["name"],
        )

    def tearDown(self):
        super().tearDown()

    def _materialize_and_get_online_store_source(
        self,
        name: str,
        store_type: str,
        table_name: str,
        input_data: pd.DataFrame,
        keys: List[str],
    ) -> OnlineStoreSource:
        online_store_sink = OnlineStoreSink(
            store_type=store_type,
            table_name=table_name,
        )

        file_source = self._create_file_source(input_data, keys=keys)

        self.processor.materialize_features(
            features=file_source,
            sink=online_store_sink,
            allow_overwrite=True,
        ).wait()

        online_store_source = OnlineStoreSource(
            name=name,
            keys=keys,
            store_type=store_type,
            table_name=table_name,
        )
        self.registry.build_features([online_store_source])

        return online_store_source

    def test_instantiate(self):
        config = flatten_dict({"feature_service": {"type": "local"}})
        feature_service = FeatureService.instantiate(
            props=config, stores=self.stores, registry=self.registry
        )
        self.assertIsInstance(feature_service, LocalFeatureService)

    def test_join_and_expression_transform(self):
        request_df = pd.DataFrame(
            [
                ["Alex", 100],
                ["Emma", 300],
            ],
            columns=["name", "extra_field"],
        )

        # Creates a FeatureView with keep_source_fields=True.
        on_demand_fv = OnDemandFeatureView(
            name="on_demand_fv",
            features=[
                f"{self.online_source_1.name}.cost",
                f"{self.online_source_2.name}.distance",
                Feature(
                    name="avg_cost",
                    dtype=types.Float32,
                    transform="cost / distance",
                ),
                Feature(
                    name="derived_extra_field",
                    dtype=types.Float32,
                    transform="distance * extra_field",
                ),
            ],
        )
        self.registry.build_features([on_demand_fv])
        online_features = self.feature_service.get_online_features(
            request_df=request_df,
            feature_view=on_demand_fv,
        )
        expected_online_features = pd.DataFrame(
            [
                ["Alex", 600, 800, 0.75, 80000],
                ["Emma", 200, 250, 0.8, 75000],
            ],
            columns=["name", "cost", "distance", "avg_cost", "derived_extra_field"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

    def test_selected_features(self):
        request_df = pd.DataFrame(
            [
                ["Alex", 100],
                ["Emma", 300],
            ],
            columns=["name", "extra_field"],
        )

        # Creates a FeatureView with keep_source_fields=True.
        on_demand_fv = OnDemandFeatureView(
            name="on_demand_fv",
            features=[
                f"{self.online_source_1.name}.cost",
                f"{self.online_source_2.name}.distance",
                Feature(
                    name="avg_cost",
                    dtype=types.Float32,
                    transform="cost / distance",
                ),
            ],
        )
        self.registry.build_features([on_demand_fv])

        online_features = self.feature_service.get_online_features(
            request_df=request_df,
            feature_view=on_demand_fv,
            feature_fields=["distance", "avg_cost"],
        )
        expected_online_features = pd.DataFrame(
            [
                [800, 0.75],
                [250, 0.8],
            ],
            columns=["distance", "avg_cost"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

    def test_keep_source_fields(self):
        request_df = pd.DataFrame(
            [
                ["Alex", 100],
                ["Emma", 300],
            ],
            columns=["name", "extra_field"],
        )

        # Creates a FeatureView with keep_source_fields=False.
        on_demand_fv = OnDemandFeatureView(
            name="on_demand_fv",
            features=[f"{self.online_source_1.name}.cost"],
            keep_source_fields=True,
        )
        self.registry.build_features([on_demand_fv])
        online_features = self.feature_service.get_online_features(
            request_df=request_df,
            feature_view=on_demand_fv,
        )
        expected_online_features = pd.DataFrame(
            [
                ["Alex", 100, 600],
                ["Emma", 300, 200],
            ],
            columns=["name", "extra_field", "cost"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

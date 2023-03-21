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
import shutil
import tempfile
import unittest
from typing import List, Optional

import pandas as pd

from feathub.common import types
from feathub.common.config import flatten_dict
from feathub.common.types import from_numpy_dtype
from feathub.feature_service.feature_service import FeatureService
from feathub.feature_service.local_feature_service import LocalFeatureService
from feathub.feature_tables.sinks.memory_store_sink import MemoryStoreSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sources.memory_store_source import MemoryStoreSource
from feathub.feature_views.feature import Feature
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.local.local_processor import LocalProcessor
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema


class FeatureServiceTest(unittest.TestCase):
    def setUp(self):
        self.registry = LocalRegistry(props={})
        self.processor = LocalProcessor(props={}, registry=self.registry)
        self.temp_dir = tempfile.mkdtemp()
        self.feature_service = LocalFeatureService(props={}, registry=self.registry)
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
            table_name="table_name_2",
            input_data=input_data_2,
            keys=["name"],
        )

    def tearDown(self):
        MemoryOnlineStore.get_instance().reset()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        timestamp_field: str = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> FileSystemSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".csv").name
        schema = Schema(
            field_names=df.keys().tolist(),
            field_types=[from_numpy_dtype(dtype) for dtype in df.dtypes],
        )
        df.to_csv(path, index=False, header=False)

        return FileSystemSource(
            name="source",
            path=path,
            data_format="csv",
            schema=schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    def _materialize_and_get_online_store_source(
        self,
        name: str,
        table_name: str,
        input_data: pd.DataFrame,
        keys: List[str],
    ) -> MemoryStoreSource:
        online_store_sink = MemoryStoreSink(table_name=table_name)

        file_source = self._create_file_source(input_data, keys=keys)

        self.processor.materialize_features(
            features=file_source,
            sink=online_store_sink,
            allow_overwrite=True,
        ).wait()

        online_store_source = MemoryStoreSource(
            name=name,
            keys=keys,
            table_name=table_name,
        )
        self.registry.build_features([online_store_source])

        return online_store_source

    def test_instantiate(self):
        config = flatten_dict({"feature_service": {"type": "local"}})
        feature_service = FeatureService.instantiate(
            props=config, registry=self.registry
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
                    transform="cost / distance",
                ),
                Feature(
                    name="derived_extra_field",
                    transform="distance * extra_field",
                ),
            ],
            request_schema=Schema.new_builder()
            .column("name", types.String)
            .column("extra_field", types.Float32)
            .build(),
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
                    transform="cost / distance",
                ),
            ],
            request_schema=Schema.new_builder()
            .column("name", types.String)
            .column("extra_field", types.Float32)
            .build(),
        )
        self.registry.build_features([on_demand_fv])

        online_features = self.feature_service.get_online_features(
            request_df=request_df,
            feature_view=on_demand_fv,
            feature_names=["distance", "avg_cost"],
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
            request_schema=Schema.new_builder()
            .column("name", types.String)
            .column("extra_field", types.Float32)
            .build(),
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

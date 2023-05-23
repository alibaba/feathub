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
import shutil
import tempfile
import unittest
from abc import ABC, abstractmethod
from typing import cast

import numpy as np
import pandas as pd

from feathub.common import types
from feathub.common.config import flatten_dict
from feathub.common.types import from_numpy_dtype
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.registries.local_registry import LocalRegistry
from feathub.registries.registry import Registry
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class RegistryTest(unittest.TestCase):
    def test_instantiate(self):
        config = flatten_dict(
            {
                "registry": {
                    "type": "local",
                    "local": {"namespace": "my-namespace"},
                }
            }
        )
        registry = Registry.instantiate(props=config)
        self.assertIsInstance(registry, LocalRegistry)
        self.assertEqual("my-namespace", cast(LocalRegistry, registry).namespace)


class RegistryTestBase(ABC, unittest.TestCase):

    # By setting this attribute to false, it prevents pytest from discovering
    # this class as a test when searching up from its child classes.
    __test__ = False

    def setUp(self) -> None:
        self.client = self._get_client()
        self.registry = self.client.registry
        self.processor = self.client.processor
        self.temp_dir = tempfile.mkdtemp()
        self.input_data = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @abstractmethod
    def _get_client(self) -> FeathubClient:
        pass

    def _create_file_source(self, df: pd.DataFrame) -> FileSystemSource:
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
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

    def test_get_features(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)
        try:
            self.registry.get_features(source.name)
            self.fail("RuntimeError should be raised.")
        except RuntimeError as err:
            self.assertTrue(
                "Table 'source' is not found in the cache or registry" in str(err)
            )

        self.registry.register_features([source])
        fetched_source = self.registry.get_features(source.name, force_update=True)
        self.assertEqual(source, fetched_source)

    def test_build_features(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            dtype=types.Float32,
            transform="cost / distance + 10",
        )
        features = DerivedFeatureView(
            name="feature_view",
            source="source",
            features=[
                f_cost_per_mile,
            ],
            keep_source_fields=True,
        )

        # get_table() should fail because 'source' is not built or registered.
        try:
            self.processor.get_table(feature_descriptor=features).to_pandas()
            self.fail("RuntimeError should be raised.")
        except RuntimeError as err:
            self.assertTrue(
                "Table 'feature_view' is not found in the cache or registry" in str(err)
            )

        # build_features() should fail because 'source' is not built or registered.
        try:
            self.registry.build_features([features])
            self.fail("RuntimeError should be raised.")
        except RuntimeError as err:
            self.assertTrue(
                "Table 'source' is not found in the cache or registry" in str(err)
            )

        self.registry.build_features([source, features])
        result_df = self.processor.get_table(feature_descriptor=features).to_pandas()

        expected_result_df = df
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        ).astype(np.float32)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_delete_features(self):
        df = self.input_data.copy()
        source: TableDescriptor = self._create_file_source(df)

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            dtype=types.Float32,
            transform="cost / distance + 10",
        )
        features: TableDescriptor = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                f_cost_per_mile,
            ],
            keep_source_fields=True,
        )

        source, features = self.registry.build_features([source, features])
        fetched_source = self.registry.get_features(source.name)
        self.assertEqual(source, fetched_source)

        self.assertTrue(self.registry.delete_features(source.name))
        self.assertFalse(self.registry.delete_features(source.name))
        try:
            self.registry.get_features(source.name)
            self.fail("RuntimeError should be raised.")
        except RuntimeError as err:
            self.assertTrue(
                "Table 'source' is not found in the cache or registry" in str(err)
            )

        # A resolved feature should still be able to work after its dependency
        # has been deleted.
        result_df = self.processor.get_table(feature_descriptor=features).to_pandas()

        expected_result_df = df
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        ).astype(np.float32)
        self.assertTrue(expected_result_df.equals(result_df))

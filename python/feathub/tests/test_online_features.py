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
from abc import ABC
from typing import List, Optional

import pandas as pd

from feathub.common import types
from feathub.common.utils import get_table_schema
from feathub.feature_tables.sinks.memory_store_sink import MemoryStoreSink
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class OnlineFeaturesITTest(ABC, FeathubITTestBase):
    def test_materialize_features_with_inconsistent_dtypes(self):
        table_name = "table_name_1"
        sink = MemoryStoreSink(table_name=table_name)

        source_1 = self.create_file_source(self.input_data, keys=["name"])
        self.client.materialize_features(
            features=source_1,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        # Inserts data with different schema to the same table.
        input_data_2 = pd.DataFrame(
            [
                ["Emma", 1200, "2022-01-01 08:04:00"],
                ["Alex", 1600, "2022-01-05 08:06:00"],
            ],
            columns=["name", "cost", "time"],
        )
        schema_2 = (
            Schema.new_builder()
            .column("name", types.String)
            .column("cost", types.Int64)
            .column("time", types.String)
            .build()
        )
        source_2 = self.create_file_source(input_data_2, schema=schema_2, keys=["name"])

        try:
            self.client.materialize_features(
                features=source_2,
                sink=sink,
                allow_overwrite=True,
            ).wait()
            self.fail("RuntimeError should be raised.")
        except RuntimeError as err:
            self.assertEqual(
                str(err),
                f"Features' dtypes {get_table_schema(source_2)} do not match with "
                f"dtypes {get_table_schema(source_1)} of the table {table_name}.",
            )

    def test_materialize_features(self):
        table_name = "table_name_1"
        # Inserts data to a new table and verifies the result.
        online_features = self._materialize_and_get_online_features(
            table_name=table_name,
            input_data=self.input_data,
            keys_to_get=pd.DataFrame(["Alex", "Emma"], columns=["name"]),
        )
        expected_online_features = pd.DataFrame(
            [
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

        # Inserts data to the existing table and verifies the result.
        input_data = pd.DataFrame(
            [
                ["Emma", 1200, 250, "2022-01-01 08:04:00"],
                ["Alex", 1600, 800, "2022-01-05 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        online_features = self._materialize_and_get_online_features(
            table_name=table_name,
            input_data=input_data,
            keys_to_get=pd.DataFrame(["Alex", "Emma"], columns=["name"]),
        )
        expected_online_features = pd.DataFrame(
            [
                ["Alex", 1600, 800, "2022-01-05 08:06:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

    def test_get_online_features_with_extra_fields_in_input_data(self):
        keys = pd.DataFrame(
            [
                ["Alex", 100],
                ["Emma", 300],
            ],
            columns=["name", "extra_field"],
        )

        online_features = self._materialize_and_get_online_features(
            table_name="table_name_1",
            input_data=self.input_data,
            keys_to_get=keys,
        )
        expected_online_features = pd.DataFrame(
            [
                ["Alex", 100, 600, 800, "2022-01-03 08:06:00"],
                ["Emma", 300, 200, 250, "2022-01-02 08:04:00"],
            ],
            columns=["name", "extra_field", "cost", "distance", "time"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

    def test_get_online_features_without_timestamp_field(self):
        online_features = self._materialize_and_get_online_features(
            table_name="table_name_1",
            input_data=self.input_data,
            keys_to_get=pd.DataFrame(["Alex", "Emma"], columns=["name"]),
            include_timestamp_field=False,
        )
        expected_online_features = pd.DataFrame(
            [
                ["Alex", 600, 800],
                ["Emma", 200, 250],
            ],
            columns=["name", "cost", "distance"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

    def test_get_online_features_with_selected_features(self):
        # Inserts data to a new table and verifies the result.
        online_features = self._materialize_and_get_online_features(
            table_name="table_name_1",
            input_data=self.input_data,
            keys_to_get=pd.DataFrame(["Alex", "Emma"], columns=["name"]),
            feature_names=["cost"],
        )
        expected_online_features = pd.DataFrame(
            [
                ["Alex", 600, "2022-01-03 08:06:00"],
                ["Emma", 200, "2022-01-02 08:04:00"],
            ],
            columns=["name", "cost", "time"],
        )
        self.assertTrue(expected_online_features.equals(online_features))

    def _materialize_and_get_online_features(
        self,
        table_name: str,
        input_data: pd.DataFrame,
        keys_to_get: pd.DataFrame,
        feature_names: Optional[List[str]] = None,
        include_timestamp_field: bool = True,
    ) -> pd.DataFrame:
        sink = MemoryStoreSink(table_name=table_name)

        source = self.create_file_source(input_data, keys=["name"])
        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        return MemoryOnlineStore.get_instance().get(
            table_name=table_name,
            input_data=keys_to_get,
            feature_names=feature_names,
            include_timestamp_field=include_timestamp_field,
        )

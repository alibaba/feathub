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

import unittest
import pandas as pd

from feathub.online_stores.memory_online_store import MemoryOnlineStore


class MemoryOnlineStoreTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.features = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-02 08:02:30"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

    def tearDown(self) -> None:
        MemoryOnlineStore.get_instance().reset()

    def test_put_and_get(self):
        store = MemoryOnlineStore.get_instance()
        store.put(
            table_name="table_1",
            features=self.features,
            key_fields=["name"],
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        keys = pd.DataFrame([["Alex"], ["Emma"]], columns=["name"])
        result_df = store.get(
            table_name="table_1",
            input_data=keys,
            feature_names=["cost"],
            include_timestamp_field=True,
        )

        expected_result_df = pd.DataFrame(
            [
                ["Alex", 300, "2022-01-02 08:03:00"],
                ["Emma", 200, "2022-01-02 08:04:00"],
            ],
            columns=["name", "cost", "time"],
        )
        self.assertTrue(expected_result_df.equals(result_df))

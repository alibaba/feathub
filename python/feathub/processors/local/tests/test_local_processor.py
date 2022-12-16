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
from datetime import datetime

from feathub.common.exceptions import FeathubException
from feathub.common.test_utils import LocalProcessorTestCase
from feathub.feature_tables.sinks.online_store_sink import OnlineStoreSink
from feathub.online_stores.memory_online_store import MemoryOnlineStore


class ProcessorTest(LocalProcessorTestCase):
    def setUp(self):
        super().setUp()
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
        super().tearDown()

    def test_get_table_with_single_key(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        keys = pd.DataFrame(
            [
                ["Alex"],
                ["Jack"],
                ["Dummy"],
            ],
            columns=["name"],
        )
        result_df = self.processor.get_table(features=source, keys=keys).to_pandas()
        expected_result_df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_with_multiple_keys(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        keys = pd.DataFrame(
            [
                ["Alex", 100],
                ["Alex", 200],
                ["Jack", 500],
                ["Dummy", 300],
            ],
            columns=["name", "cost"],
        )
        result_df = self.processor.get_table(features=source, keys=keys).to_pandas()
        expected_result_df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_with_start_datetime(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        start_datetime = datetime.strptime("2022-01-02 08:03:00", "%Y-%m-%d %H:%M:%S")

        result_df = self.processor.get_table(
            features=source, start_datetime=start_datetime
        ).to_pandas()
        expected_result_df = pd.DataFrame(
            [
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_with_end_datetime(self):
        df = self.input_data.copy()
        source = self._create_file_source(df, keys=["name"])
        end_datetime = datetime.strptime("2022-01-02 08:03:00", "%Y-%m-%d %H:%M:%S")

        result_df = self.processor.get_table(
            features=source, end_datetime=end_datetime
        ).to_pandas()
        expected_result_df = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_get_table_missing_timestamp(self):
        df = self.input_data.copy()
        df = df.drop(columns=["time"])
        source = self._create_file_source(df, keys=["name"], timestamp_field=None)
        end_datetime = datetime.strptime("2022-01-02 08:03:00", "%Y-%m-%d %H:%M:%S")

        try:
            self.processor.get_table(
                features=source, end_datetime=end_datetime
            ).to_pandas()
            self.fail("RuntimeError should be raised.")
        except FeathubException as err:
            self.assertEqual(str(err), "Features do not have timestamp column.")

    def test_materialize_features_with_inconsistent_dtypes(self):
        table_name = "table_name_1"
        sink = OnlineStoreSink(
            store_type=MemoryOnlineStore.STORE_TYPE,
            table_name=table_name,
        )

        source_1 = self._create_file_source(self.input_data, keys=["name"])
        self.processor.materialize_features(
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
        source_2 = self._create_file_source(input_data_2, keys=["name"])

        try:
            self.processor.materialize_features(
                features=source_2,
                sink=sink,
                allow_overwrite=True,
            ).wait()
            self.fail("RuntimeError should be raised.")
        except RuntimeError as err:
            self.assertEqual(
                str(err),
                f"Features' dtypes {input_data_2.dtypes.to_dict()} do not "
                f"match with table {table_name}.",
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

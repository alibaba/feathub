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
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
import sqlalchemy
from pandas._testing import assert_frame_equal

from feathub.common import types
from feathub.feature_tables.sinks.mysql_sink import MySQLSink
from feathub.feature_tables.sources.mysql_source import MySQLSource
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class MySQLSourceSinkITTest(ABC, FeathubITTestBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.setup_mysql_container()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.teardown_mysql_container()

    def test_mysql_sink_upsert(self):
        self.client = self.get_client(
            extra_config={"processor": {"flink": {"native.parallelism.default": "1"}}}
        )

        table_name = self.generate_random_name("test_mysql_sink_upsert")
        input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
                [1, 4, datetime(2022, 1, 1, 0, 0, 3).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=["id", "val", "ts"],
        )
        schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )

        self._initialize_mysql_table(
            input_data=input_data,
            schema=schema,
            keys=["id"],
            timestamp_field="ts",
            table_name=table_name,
        )

        expected_result = (
            pd.DataFrame(
                [
                    [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                    [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
                    [1, 4, datetime(2022, 1, 1, 0, 0, 3).strftime("%Y-%m-%d %H:%M:%S")],
                ],
                columns=["id", "val", "ts"],
            )
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        engine = sqlalchemy.create_engine(self.mysql_container.get_connection_url())
        with engine.begin() as conn:
            result = conn.execute(sqlalchemy.text(f"SELECT * FROM {table_name}"))
            r = result.fetchall()
        result_df = (
            pd.DataFrame(data=r, columns=["id", "val", "ts"])
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )
        assert_frame_equal(expected_result, result_df)

    def test_get_online_features(self):
        table_name = self.generate_random_name("test_get_online_features")
        input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=["id", "val", "ts"],
        )
        schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )
        self._initialize_mysql_table(
            input_data=input_data,
            schema=schema,
            keys=["id"],
            timestamp_field="ts",
            table_name=table_name,
        )

        source = MySQLSource(
            name=table_name,
            database=self.mysql_container.MYSQL_DATABASE,
            table=table_name,
            schema=schema,
            host=self.mysql_container.get_container_host_ip(),
            port=int(
                self.mysql_container.get_exposed_port(
                    self.mysql_container.port_to_expose
                )
            ),
            username=self.mysql_container.MYSQL_USER,
            password=self.mysql_container.MYSQL_PASSWORD,
            keys=["id"],
            timestamp_field="ts",
        )
        self.on_demand_feature_view = OnDemandFeatureView(
            name="on_demand_feature_view",
            features=[
                f"{source.name}.val",
            ],
            keep_source_fields=True,
            request_schema=Schema.new_builder().column("id", types.Int64).build(),
        )
        self.client.build_features([source, self.on_demand_feature_view])

        request_df = pd.DataFrame(
            np.array([[1, "1"], [3, "3"]]), columns=["id", "id_str"]
        ).astype({"id": "int64", "id_str": str})
        online_features = (
            self.client.get_online_features(
                request_df=request_df,
                feature_view=self.on_demand_feature_view,
            )
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result = (
            pd.DataFrame(
                [[1, "1", 1], [3, "3", 3]],
                columns=["id", "id_str", "val"],
            )
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )
        assert_frame_equal(expected_result, online_features)

    def _initialize_mysql_table(
        self,
        input_data: pd.DataFrame,
        schema: Schema,
        keys: List[str],
        timestamp_field: str,
        table_name: str,
    ):
        engine = sqlalchemy.create_engine(self.mysql_container.get_connection_url())
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"CREATE TABLE {table_name} ("
                    f"id BIGINT PRIMARY KEY, val BIGINT, ts TEXT)"
                )
            )

        file_source = self.create_file_source(
            input_data,
            keys=keys,
            schema=schema,
            timestamp_field=timestamp_field,
        )
        sink = self._get_mysql_sink(table_name)
        self.client.materialize_features(file_source, sink, allow_overwrite=True).wait(
            30_000
        )

    def _get_mysql_sink(self, table_name):
        return MySQLSink(
            database=self.mysql_container.MYSQL_DATABASE,
            table=table_name,
            host=self.mysql_container.get_container_host_ip(),
            port=int(
                self.mysql_container.get_exposed_port(
                    self.mysql_container.port_to_expose
                )
            ),
            username=self.mysql_container.MYSQL_USER,
            password=self.mysql_container.MYSQL_PASSWORD,
        )

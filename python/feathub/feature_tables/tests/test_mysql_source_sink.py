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
from typing import Optional

import pandas as pd
import sqlalchemy
from pandas._testing import assert_frame_equal
from testcontainers.mysql import MySqlContainer

from feathub.common import types
from feathub.feature_tables.sinks.mysql_sink import MySQLSink
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class MySQLSourceSinkITTest(ABC, FeathubITTestBase):
    mysql_container: Optional[MySqlContainer] = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.mysql_container = MySqlContainer(image="mysql:8.0")
        cls.mysql_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.mysql_container.stop()

    def test_mysql_sink(self):
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

        source = self.create_file_source(
            input_data,
            keys=["id"],
            schema=schema,
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        table_name = self._testMethodName

        engine = sqlalchemy.create_engine(self.mysql_container.get_connection_url())
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"CREATE TABLE {table_name} ("
                    f"id BIGINT PRIMARY KEY, val BIGINT, ts TEXT)"
                )
            )

        sink = self._get_mysql_sink(table_name)

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait(30000)

        with engine.begin() as conn:
            result = conn.execute(sqlalchemy.text(f"SELECT * FROM {table_name}"))
            r = result.fetchall()

        result_df = (
            pd.DataFrame(data=r, columns=["id", "val", "ts"])
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )
        assert_frame_equal(
            input_data.sort_values(by=["id"]).reset_index(drop=True), result_df
        )

    def test_mysql_sink_upsert(self):
        self.client = self.get_client(
            extra_config={"processor": {"flink": {"native.parallelism.default": "1"}}}
        )

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

        source = self.create_file_source(
            input_data,
            keys=["id"],
            schema=schema,
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        table_name = self._testMethodName

        engine = sqlalchemy.create_engine(self.mysql_container.get_connection_url())
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"CREATE TABLE {table_name} ("
                    f"id BIGINT PRIMARY KEY, val BIGINT, ts TEXT)"
                )
            )

        sink = self._get_mysql_sink(table_name)

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait(30000)

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

        with engine.begin() as conn:
            result = conn.execute(sqlalchemy.text(f"SELECT * FROM {table_name}"))
            r = result.fetchall()
        result_df = (
            pd.DataFrame(data=r, columns=["id", "val", "ts"])
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )
        print(result_df)
        assert_frame_equal(expected_result, result_df)

    def _get_mysql_sink(self, table_name):
        return MySQLSink(
            host=self.mysql_container.get_container_host_ip(),
            port=int(
                self.mysql_container.get_exposed_port(
                    self.mysql_container.port_to_expose
                )
            ),
            database=self.mysql_container.MYSQL_DATABASE,
            username=self.mysql_container.MYSQL_USER,
            password=self.mysql_container.MYSQL_PASSWORD,
            table=table_name,
        )

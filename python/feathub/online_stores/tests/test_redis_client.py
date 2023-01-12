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
from datetime import datetime

import numpy as np
import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment
from testcontainers.redis import RedisContainer

from feathub.common import types
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.feature_tables.sources.redis_source import RedisSource
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView
from feathub.processors.flink.table_builder.source_sink_utils import insert_into_sink
from feathub.processors.flink.table_builder.tests.mock_table_descriptor import (
    MockTableDescriptor,
)
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


# TODO: Restructure these test cases in a way similar to ProcessorTestBase.
class RedisClientTest(unittest.TestCase):
    redis_container: RedisContainer = None

    def __init__(self, method_name: str):
        super().__init__(method_name)

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.redis_container = RedisContainer()
        cls.redis_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.redis_container.stop()

    def setUp(self) -> None:
        super().setUp()
        self.host = RedisClientTest.redis_container.get_container_host_ip()
        if self.host == "localhost":
            self.host = "127.0.0.1"
        self.port = RedisClientTest.redis_container.get_exposed_port(
            RedisClientTest.redis_container.port_to_expose
        )

        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.t_env = StreamTableEnvironment.create(self.env)
        self.test_time = datetime.now()

        self.row_data = self._produce_data_to_redis(self.t_env)

        self.client = FeathubClient(props={})

        self.schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )

        self.on_demand_feature_view = OnDemandFeatureView(
            name="on_demand_feature_view",
            features=[
                "table_name_1.val",
            ],
            keep_source_fields=True,
        )

    def test_get_online_features(self):
        source = RedisSource(
            name="table_name_1",
            host=self.host,
            port=int(self.port),
            schema=self.schema,
            keys=["id"],
            timestamp_field="ts",
        )

        self.client.build_features([source, self.on_demand_feature_view])

        request_df = pd.DataFrame(np.array([[1], [2], [3]]), columns=["id"])
        online_features = self.client.get_online_features(
            request_df=request_df,
            feature_view=self.on_demand_feature_view,
        )

        rows = [x for _, x in online_features.iterrows()]
        self.assertEquals(3, len(rows))

        for i in range(len(rows)):
            row = rows[i]
            self.assertEquals({"id": i + 1, "val": i + 2}, row.to_dict())

    def test_more_input_column_than_keys(self):
        source = RedisSource(
            name="table_name_1",
            host=self.host,
            port=int(self.port),
            schema=self.schema,
            keys=["id"],
            timestamp_field="ts",
        )

        self.client.build_features([source, self.on_demand_feature_view])

        request_df = pd.DataFrame(
            np.array([[1, "1"], [2, "2"], [3, "3"]]), columns=["id", "id_str"]
        )
        request_df["id"] = pd.to_numeric(request_df["id"])
        online_features = self.client.get_online_features(
            request_df=request_df,
            feature_view=self.on_demand_feature_view,
        )

        rows = [x for _, x in online_features.iterrows()]
        self.assertEquals(3, len(rows))

        for i in range(len(rows)):
            row = rows[i]
            self.assertEquals(
                {"id": i + 1, "id_str": str(i + 1), "val": i + 2}, row.to_dict()
            )

    def _produce_data_to_redis(self, t_env):
        row_data = [
            (1, 2, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")),
            (2, 3, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")),
            (3, 4, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")),
        ]
        table = t_env.from_elements(
            row_data,
            DataTypes.ROW(
                [
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("val", DataTypes.BIGINT()),
                    DataTypes.FIELD("ts", DataTypes.STRING()),
                ]
            ),
        )

        sink = RedisSink(
            host=self.host,
            port=int(self.port),
        )

        descriptor: TableDescriptor = MockTableDescriptor(
            keys=["id"], timestamp_field="ts", timestamp_format="%Y-%m-%d %H:%M:%S"
        )

        insert_into_sink(t_env, table, descriptor, sink).wait(30000)
        return row_data

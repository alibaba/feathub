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
import os
import unittest
from abc import abstractmethod
from datetime import datetime

import numpy as np
import pandas as pd
from pyflink import java_gateway
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment
from testcontainers.redis import RedisContainer

from feathub.common import types
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.redis_sink import RedisSink, RedisMode
from feathub.feature_tables.sources.redis_source import RedisSource
from feathub.feature_tables.tests.test_redis_source_sink import RedisClusterContainer
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView
from feathub.processors.flink.table_builder.source_sink_utils import (
    add_sink_to_statement_set,
)
from feathub.processors.flink.table_builder.tests.mock_table_descriptor import (
    MockTableDescriptor,
)
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


# TODO: Restructure these test cases in a way similar to ProcessorTestBase.
# TODO: move this class to python/feathub/online_stores/tests folder after
#  the resource leak problem of other flink processor's tests is fixed.
class RedisClientTestBase(unittest.TestCase):
    # By setting this attribute to false, it prevents pytest from discovering
    # this class as a test when searching up from its child classes.
    __test__ = False

    def __init__(self, method_name: str):
        super().__init__(method_name)

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        # TODO: replace the cleanup below with flink configuration after
        #  pyflink dependency upgrades to 1.16.0 or higher. Related
        #  ticket: FLINK-27297
        with java_gateway._lock:
            if java_gateway._gateway is not None:
                java_gateway._gateway.shutdown()
                java_gateway._gateway = None
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")

    @abstractmethod
    def get_port(self):
        pass

    @abstractmethod
    def get_mode(self):
        pass

    def setUp(self) -> None:
        super().setUp()
        self.host = "127.0.0.1"
        self.port = self.get_port()

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
            .column("map", types.MapType(types.String, types.Bool))
            .column("list", types.Float64Vector)
            .column(
                "nested_map",
                types.MapType(types.String, types.MapType(types.String, types.Bool)),
            )
            .column("nested_list", types.MapType(types.String, types.Float64Vector))
            .build()
        )

        self.on_demand_feature_view = OnDemandFeatureView(
            name="on_demand_feature_view",
            features=[
                "table_name_1.val",
                "table_name_1.map",
                "table_name_1.list",
                "table_name_1.nested_map",
                "table_name_1.nested_list",
            ],
            keep_source_fields=True,
            request_schema=Schema.new_builder().column("id", types.Int64).build(),
        )

    def test_get_online_features(self):
        source = RedisSource(
            name="table_name_1",
            host=self.host,
            port=int(self.port),
            mode=self.get_mode(),
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
            self.assertEquals(
                {
                    "id": i + 1,
                    "val": i + 2,
                    "map": {"key": i % 2 == 0},
                    "list": [i + 1.0, i + 2.0],
                    "nested_map": {"key": {"key": i % 2 == 0}},
                    "nested_list": {"key": [i + 1.0, i + 2.0]},
                },
                row.to_dict(),
            )

    def test_more_input_column_than_keys(self):
        source = RedisSource(
            name="table_name_1",
            host=self.host,
            port=int(self.port),
            mode=self.get_mode(),
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
                {
                    "id": i + 1,
                    "id_str": str(i + 1),
                    "val": i + 2,
                    "map": {"key": i % 2 == 0},
                    "list": [i + 1.0, i + 2.0],
                    "nested_map": {"key": {"key": i % 2 == 0}},
                    "nested_list": {"key": [i + 1.0, i + 2.0]},
                },
                row.to_dict(),
            )

    def _produce_data_to_redis(self, t_env):
        row_data = [
            [
                1,
                2,
                datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S"),
                {"key": True},
                [1.0, 2.0],
                {"key": {"key": True}},
                {"key": [1.0, 2.0]},
            ],
            [
                2,
                3,
                datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S"),
                {"key": False},
                [2.0, 3.0],
                {"key": {"key": False}},
                {"key": [2.0, 3.0]},
            ],
            [
                3,
                4,
                datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S"),
                {"key": True},
                [3.0, 4.0],
                {"key": {"key": True}},
                {"key": [3.0, 4.0]},
            ],
        ]
        table = t_env.from_elements(
            row_data,
            DataTypes.ROW(
                [
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("val", DataTypes.BIGINT()),
                    DataTypes.FIELD("ts", DataTypes.STRING()),
                    DataTypes.FIELD(
                        "map", DataTypes.MAP(DataTypes.STRING(), DataTypes.BOOLEAN())
                    ),
                    DataTypes.FIELD("list", DataTypes.ARRAY(DataTypes.DOUBLE())),
                    DataTypes.FIELD(
                        "nested_map",
                        DataTypes.MAP(
                            DataTypes.STRING(),
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.BOOLEAN()),
                        ),
                    ),
                    DataTypes.FIELD(
                        "nested_list",
                        DataTypes.MAP(
                            DataTypes.STRING(), DataTypes.ARRAY(DataTypes.DOUBLE())
                        ),
                    ),
                ]
            ),
        )

        sink = RedisSink(
            mode=self.get_mode(),
            host=self.host,
            port=int(self.port),
        )

        descriptor: TableDescriptor = MockTableDescriptor(
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            output_feature_names=[
                "id",
                "val",
                "ts",
                "map",
                "list",
                "nested_map",
                "nested_list",
            ],
        )

        statement_set = t_env.create_statement_set()
        add_sink_to_statement_set(t_env, statement_set, table, descriptor, sink)
        statement_set.execute().wait(30000)
        return row_data


class RedisClientTest(RedisClientTestBase):
    __test__ = True

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

    def get_port(self):
        return RedisClientTest.redis_container.get_exposed_port(
            RedisClientTest.redis_container.port_to_expose
        )

    def get_mode(self):
        return RedisMode.STANDALONE


class RedisClientClusterModeTest(RedisClientTestBase):
    __test__ = True

    redis_container: RedisClusterContainer = None

    def __init__(self, method_name: str):
        super().__init__(method_name)

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.redis_container = RedisClusterContainer()
        cls.redis_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.redis_container.stop()

    def get_port(self):
        return 7000

    def get_mode(self):
        return RedisMode.CLUSTER

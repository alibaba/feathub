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
from abc import ABC
from datetime import datetime
from typing import Union, Any

import pandas as pd
import redis
from redis import Redis
from redis import RedisCluster
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.redis import RedisContainer

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.feature_tables.sources.redis_source import (
    RedisSource,
)
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.online_stores.conversion_utils import to_python_object
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class RedisClusterContainer(DockerContainer):
    def __init__(self, image="grokzen/redis-cluster:7.0.10", **kwargs):
        super(RedisClusterContainer, self).__init__(image, **kwargs)
        self.with_env("IP", "0.0.0.0")
        for port in range(7000, 7003):
            self.with_bind_ports(port, port)

    @wait_container_is_ready(
        redis.exceptions.ConnectionError,
        redis.exceptions.RedisClusterException,
        IndexError,
    )
    def _wait_container_ready(self):
        client = self.get_client()
        if not client.ping():
            raise redis.exceptions.ConnectionError("Could not connect to Redis")
        client.close()

    def get_client(self):
        return RedisCluster(host="127.0.0.1", port=7000)

    def clear_all_keys(self):
        for port in range(7000, 7003):
            client = Redis(host="127.0.0.1", port=port)
            cluster_client: RedisCluster = RedisCluster(host="127.0.0.1", port=port)
            existing_keys = client.keys()
            if len(existing_keys) > 0:
                cluster_client.delete(*existing_keys)
            client.close()
            cluster_client.close()

    def start(self):
        super().start()
        self._wait_container_ready()
        return self


def _generate_test_data():
    dataframe_data = pd.DataFrame(
        [
            [
                1,
                1,
                datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S"),
                {"key": True},
                [1.0, 2.0],
                {"key": {"key": True}},
                {"key": [1.0, 2.0]},
            ],
            [
                2,
                2,
                datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S"),
                {"key": False},
                [2.0, 3.0],
                {"key": {"key": False}},
                {"key": [2.0, 3.0]},
            ],
            [
                3,
                3,
                datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S"),
                {"key": True},
                [3.0, 4.0],
                {"key": {"key": True}},
                {"key": [3.0, 4.0]},
            ],
        ],
        columns=["id", "val", "ts", "map", "list", "nested_map", "nested_list"],
    )

    redis_data = [
        (b"test_namespace:1:val", b"1"),
        (b"test_namespace:1:ts", b"2022-01-01 00:00:00"),
        (b"test_namespace:1:map", {b"key": b"true"}),
        (b"test_namespace:1:list", [b"1.0", b"2.0"]),
        (b"test_namespace:1:nested_map", {b"key": b'{"key":"true"}'}),
        (b"test_namespace:1:nested_list", {b"key": b'["1.0","2.0"]'}),
        (b"test_namespace:2:val", b"2"),
        (b"test_namespace:2:ts", b"2022-01-01 00:00:01"),
        (b"test_namespace:2:map", {b"key": b"false"}),
        (b"test_namespace:2:list", [b"2.0", b"3.0"]),
        (b"test_namespace:2:nested_map", {b"key": b'{"key":"false"}'}),
        (b"test_namespace:2:nested_list", {b"key": b'["2.0","3.0"]'}),
        (b"test_namespace:3:val", b"3"),
        (b"test_namespace:3:ts", b"2022-01-01 00:00:02"),
        (b"test_namespace:3:map", {b"key": b"true"}),
        (b"test_namespace:3:list", [b"3.0", b"4.0"]),
        (b"test_namespace:3:nested_map", {b"key": b'{"key":"true"}'}),
        (b"test_namespace:3:nested_list", {b"key": b'["3.0","4.0"]'}),
    ]

    schema = (
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

    return dataframe_data, redis_data, schema


def _test_redis_sink(
    self: FeathubITTestBase,
    host: str,
    port: int,
    mode: str,
    redis_client: Union[Redis, RedisCluster],
):
    # TODO: Fix the bug that in flink processor when column "val"
    #  contains None, all values in this column are saved as None
    #  to Redis.
    dataframe_data, redis_data, schema = _generate_test_data()

    source = self.create_file_source(
        dataframe_data,
        keys=["id"],
        schema=schema,
        timestamp_field="ts",
        timestamp_format="%Y-%m-%d %H:%M:%S",
        data_format="json",
    )

    sink = RedisSink(
        namespace="test_namespace",
        mode=mode,
        host=host,
        port=port,
    )

    self.client.materialize_features(
        feature_descriptor=source, sink=sink, allow_overwrite=True
    ).wait(30000)

    if not isinstance(redis_client, RedisCluster):
        # Cluster client do not scan all nodes in the KEYS command
        self.assertEquals(len(redis_client.keys("*")), dataframe_data.shape[0] * 6)

    for data in redis_data:
        feature_name = data[0].decode("utf-8").split(":")[-1]
        field_type = schema.get_field_type(feature_name)
        keys = data[0].decode("utf-8").split(":")[1]

        if isinstance(field_type, types.VectorType):
            actual_result: Any = redis_client.lrange(data[0], 0, -1)
        elif isinstance(field_type, types.MapType):
            actual_result = redis_client.hgetall(data[0])
        else:
            actual_result = redis_client.get(data[0])

        self.assertEquals(
            dataframe_data[feature_name][int(keys) - 1],
            to_python_object(actual_result, field_type),
        )

    redis_client.close()


def _test_redis_sink_update_entry(
    self: FeathubITTestBase,
    host: str,
    port: int,
    mode: str,
    redis_client: Union[Redis, RedisCluster],
):
    result_data = [
        1,
        2,
        {"key2": False, "key3": True},
        [3.0, 4.0, 5.0],
    ]

    initial_data_overlapping_result_data = [
        1,
        1,
        {"key1": False, "key2": True},
        [1.0, 2.0, 3.0],
    ]

    initial_data_containing_result_data = [
        1,
        1,
        {"key1": False, "key2": True, "key3": False, "key4": True},
        [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
    ]

    initial_date_contained_by_result_data = [
        1,
        1,
        {"key2": True},
        [4.0],
    ]

    schema = (
        Schema.new_builder()
        .column("id", types.Int64)
        .column("val", types.Int32)
        .column("map", types.MapType(types.String, types.Bool))
        .column("list", types.Float64Vector)
        .build()
    )

    sink = RedisSink(
        namespace="test_namespace",
        mode=mode,
        host=host,
        port=port,
    )

    for initial_data in [
        initial_data_overlapping_result_data,
        initial_data_containing_result_data,
        initial_date_contained_by_result_data,
    ]:
        input_data = pd.DataFrame(
            [initial_data, result_data],
            columns=["id", "val", "map", "list"],
        )

        source = self.create_file_source(
            input_data,
            keys=["id"],
            schema=schema,
            timestamp_field=None,
            data_format="json",
        )

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait(30000)

        if not isinstance(redis_client, RedisCluster):
            # Cluster client do not scan all nodes in the KEYS command
            self.assertEquals(len(redis_client.keys("*")), 3)

        self.assertEquals(redis_client.get("test_namespace:1:val"), b"2")
        self.assertEquals(
            redis_client.hgetall("test_namespace:1:map"),
            {b"key2": b"false", b"key3": b"true"},
        )
        self.assertEquals(
            redis_client.lrange("test_namespace:1:list", 0, -1),
            [b"3.0", b"4.0", b"5.0"],
        )

    redis_client.close()


def _test_redis_sink_custom_key(
    self: FeathubITTestBase,
    host: str,
    port: int,
    mode: str,
    redis_client: Union[Redis, RedisCluster],
):
    # TODO: Fix the bug that in flink processor when column "val"
    #  contains None, all values in this column are saved as None
    #  to Redis.
    input_data = pd.DataFrame(
        [
            [1, 1, [1.0, 2.0]],
            [2, 2, [2.0, 3.0]],
            [3, 3, [3.0, 4.0]],
        ],
        columns=["id", "val", "list"],
    )

    schema = (
        Schema.new_builder()
        .column("id", types.Int64)
        .column("val", types.Int32)
        .column("list", types.Float64Vector)
        .build()
    )

    source = self.create_file_source(
        input_data,
        keys=["id"],
        schema=schema,
        timestamp_field=None,
        data_format="json",
    )

    sink = RedisSink(
        mode=mode,
        host=host,
        port=port,
        key_expr="CONCAT(__NAMESPACE__, CAST(id AS STRING), __FEATURE_NAME__)",
    )

    self.client.materialize_features(
        feature_descriptor=source, sink=sink, allow_overwrite=True
    ).wait(30000)

    if not isinstance(redis_client, RedisCluster):
        # Cluster client do not scan all nodes in the KEYS command
        self.assertEquals(len(redis_client.keys("*")), input_data.shape[0] * 2)

    for i in range(input_data.shape[0]):
        self.assertEquals(
            redis_client.get(f"default{i+1}val"), f"{i+1}".encode("utf-8")
        )
        self.assertEquals(
            redis_client.lrange(f"default{i+1}list", 0, -1),
            [str(i + 1.0).encode("utf-8"), str(i + 2.0).encode("utf-8")],
        )

    redis_client.close()


def _test_redis_source_join(
    self: FeathubITTestBase,
    host: str,
    port: int,
    mode: str,
    redis_client: Union[Redis, RedisCluster],
):
    expected_result_df, redis_data, schema = _generate_test_data()
    for data in redis_data:
        if isinstance(data[1], dict):
            for key, value in data[1].items():
                redis_client.hset(data[0], key, value)
        elif isinstance(data[1], list):
            redis_client.rpush(data[0], *data[1])
        else:
            redis_client.set(data[0], data[1])

    redis_source = RedisSource(
        name="redis_source",
        namespace="test_namespace",
        mode=mode,
        host=host,
        port=port,
        keys=["id"],
        schema=schema,
    )

    input_data = pd.DataFrame(
        [[1], [2], [3]],
        columns=["id"],
    )

    schema = Schema.new_builder().column("id", types.Int64).build()

    source = self.create_file_source(
        df=input_data,
        keys=["id"],
        schema=schema,
        timestamp_field=None,
        data_format="csv",
    )

    feature_view = DerivedFeatureView(
        name="feature_view",
        source=source,
        features=[
            "id",
            f"{redis_source.name}.val",
            f"{redis_source.name}.ts",
            f"{redis_source.name}.map",
            f"{redis_source.name}.list",
            f"{redis_source.name}.nested_map",
            f"{redis_source.name}.nested_list",
        ],
        keep_source_fields=False,
    )

    [_, built_feature_view] = self.client.build_features([redis_source, feature_view])

    result_df = (
        self.client.get_features(feature_descriptor=built_feature_view)
        .to_pandas()
        .sort_values(by=["id"])
        .reset_index(drop=True)
    )

    self.assertTrue(result_df.equals(expected_result_df))

    redis_client.close()


def _test_redis_source_join_different_key_order(
    self: FeathubITTestBase,
    host: str,
    port: int,
    mode: str,
    redis_client: Union[Redis, RedisCluster],
):
    redis_client.set("test_namespace:Alex:ItemA:cost", "100")
    redis_client.set("test_namespace:Alex:ItemB:cost", "300")
    redis_client.set("test_namespace:Emma:ItemA:cost", "400")
    redis_client.set("test_namespace:Emma:ItemB:cost", "250")

    redis_source = RedisSource(
        name="redis_source",
        namespace="test_namespace",
        mode=mode,
        host=host,
        port=port,
        keys=["name", "item"],
        schema=(
            Schema.new_builder()
            .column("name", types.String)
            .column("item", types.String)
            .column("cost", types.Int64)
            .build()
        ),
    )

    input_data = pd.DataFrame(
        [
            ["Alex", "ItemA", "2022-01-01 08:01:00"],
            ["Emma", "ItemB", "2022-01-01 08:02:00"],
            ["Alex", "ItemB", "2022-01-03 08:03:00"],
        ],
        columns=["name", "item", "time"],
    )

    schema = (
        Schema.new_builder()
        .column("name", types.String)
        .column("item", types.String)
        .column("time", types.String)
        .build()
    )
    source = self.create_file_source(
        df=input_data,
        keys=["item", "name"],
        schema=schema,
        data_format="csv",
    )

    feature_view = DerivedFeatureView(
        name="feature_view",
        source=source,
        features=[
            "name",
            "item",
            f"{redis_source.name}.cost",
            "time",
        ],
        keep_source_fields=False,
    )

    [_, built_feature_view] = self.client.build_features([redis_source, feature_view])

    result_df = (
        self.client.get_features(feature_descriptor=built_feature_view)
        .to_pandas()
        .sort_values(by=["name"])
        .reset_index(drop=True)
    )

    expected_result = pd.DataFrame(
        [
            ["Alex", "ItemA", 100, "2022-01-01 08:01:00"],
            ["Alex", "ItemB", 300, "2022-01-03 08:03:00"],
            ["Emma", "ItemB", 250, "2022-01-01 08:02:00"],
        ],
        columns=["name", "item", "cost", "time"],
    )

    self.assertTrue(result_df.equals(expected_result))

    redis_client.close()


def _test_redis_source_join_custom_key_derived_feature_view(
    self: FeathubITTestBase,
    host: str,
    port: int,
    mode: str,
    redis_client: Union[Redis, RedisCluster],
):
    redis_client.set("test_namespace-ItemA-Alex-cost", "100")
    redis_client.set("test_namespace-ItemB-Alex-cost", "300")
    redis_client.set("test_namespace-ItemA-Emma-cost", "400")
    redis_client.set("test_namespace-ItemB-Emma-cost", "250")

    redis_source = RedisSource(
        name="redis_source",
        namespace="test_namespace",
        mode=mode,
        host=host,
        port=port,
        keys=["name", "item"],
        key_expr='CONCAT_WS("-", __NAMESPACE__, item, name, __FEATURE_NAME__)',
        schema=(
            Schema.new_builder()
            .column("name", types.String)
            .column("item", types.String)
            .column("cost", types.Int64)
            .build()
        ),
    )

    redis_feature_view = DerivedFeatureView(
        name="redis_feature_view",
        source=redis_source,
        features=[
            "LOWER(name) AS lower_name",
        ],
        keep_source_fields=True,
    )

    input_data = pd.DataFrame(
        [
            ["Alex", "ItemA", "2022-01-01 08:01:00"],
            ["Emma", "ItemB", "2022-01-01 08:02:00"],
            ["Alex", "ItemB", "2022-01-03 08:03:00"],
        ],
        columns=["name", "item", "time"],
    )

    schema = (
        Schema.new_builder()
        .column("name", types.String)
        .column("item", types.String)
        .column("time", types.String)
        .build()
    )

    source = self.create_file_source(
        df=input_data,
        keys=["item", "name"],
        schema=schema,
        data_format="csv",
    )

    feature_view = DerivedFeatureView(
        name="feature_view",
        source=source,
        features=[
            "name",
            "item",
            f"{redis_feature_view.name}.cost",
            f"{redis_feature_view.name}.lower_name",
            "time",
        ],
        keep_source_fields=False,
    )

    [_, built_feature_view] = self.client.build_features(
        [redis_feature_view, feature_view]
    )

    result_df = (
        self.client.get_features(feature_descriptor=built_feature_view)
        .to_pandas()
        .sort_values(by=["name"])
        .reset_index(drop=True)
    )

    expected_result = pd.DataFrame(
        [
            ["Alex", "ItemA", 100, "alex", "2022-01-01 08:01:00"],
            ["Alex", "ItemB", 300, "alex", "2022-01-03 08:03:00"],
            ["Emma", "ItemB", 250, "emma", "2022-01-01 08:02:00"],
        ],
        columns=["name", "item", "cost", "lower_name", "time"],
    )

    self.assertTrue(result_df.equals(expected_result))

    redis_client.close()


class RedisSourceSinkStandaloneModeITTest(ABC, FeathubITTestBase):
    redis_container: RedisContainer

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.redis_container = RedisContainer()
        cls.redis_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.redis_container.stop()

    def tearDown(self) -> None:
        super().tearDown()
        redis_client = self.redis_container.get_client()
        existing_keys = redis_client.keys("*")
        if len(existing_keys) > 0:
            redis_client.delete(*existing_keys)
        redis_client.close()

    def test_redis_sink_standalone_mode(self):
        _test_redis_sink(
            self,
            "127.0.0.1",
            int(
                self.redis_container.get_exposed_port(
                    self.redis_container.port_to_expose
                )
            ),
            "standalone",
            self.redis_container.get_client(),
        )

    def test_redis_sink_update_entry_standalone_mode(self):
        _test_redis_sink_update_entry(
            self,
            "127.0.0.1",
            int(
                self.redis_container.get_exposed_port(
                    self.redis_container.port_to_expose
                )
            ),
            "standalone",
            self.redis_container.get_client(),
        )

    def test_redis_sink_custom_key_standalone_mode(self):
        _test_redis_sink_custom_key(
            self,
            "127.0.0.1",
            int(
                self.redis_container.get_exposed_port(
                    self.redis_container.port_to_expose
                )
            ),
            "standalone",
            self.redis_container.get_client(),
        )

    def test_illegal_key_expr(self):
        try:
            RedisSink(
                namespace="test_namespace",
                host="127.0.0.1",
                port=6379,
                password="123456",
                db_num=3,
                key_expr="__FEATURE_NAME__",
            )
            self.fail("FeathubException should be raised.")
        except FeathubException as err:
            self.assertEqual(
                str(err),
                "key_expr __FEATURE_NAME__ should contain __NAMESPACE__ and "
                "__FEATURE_NAME__ in order to guarantee the uniqueness of "
                "feature keys in Redis.",
            )

    def test_redis_source_join_standalone_mode(self):
        _test_redis_source_join(
            self,
            "127.0.0.1",
            int(
                self.redis_container.get_exposed_port(
                    self.redis_container.port_to_expose
                )
            ),
            "standalone",
            self.redis_container.get_client(),
        )

    def test_redis_source_join_different_key_order_standalone_mode(self):
        _test_redis_source_join_different_key_order(
            self,
            "127.0.0.1",
            int(
                self.redis_container.get_exposed_port(
                    self.redis_container.port_to_expose
                )
            ),
            "standalone",
            self.redis_container.get_client(),
        )

    def _test_redis_source_join_custom_key_derived_feature_view_standalone_mode(
        self,
    ):
        _test_redis_source_join_custom_key_derived_feature_view(
            self,
            "127.0.0.1",
            int(
                self.redis_container.get_exposed_port(
                    self.redis_container.port_to_expose
                )
            ),
            "standalone",
            self.redis_container.get_client(),
        )


class RedisSourceSinkClusterModeITTest(ABC, FeathubITTestBase):
    redis_cluster_container: RedisClusterContainer

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.redis_cluster_container = RedisClusterContainer()
        cls.redis_cluster_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.redis_cluster_container.stop()

    def tearDown(self) -> None:
        super().tearDown()
        self.redis_cluster_container.clear_all_keys()

    def test_redis_sink_cluster_mode(self):
        _test_redis_sink(
            self,
            "127.0.0.1",
            7000,
            "cluster",
            self.redis_cluster_container.get_client(),
        )

    def test_redis_sink_update_entry_cluster_mode(self):
        _test_redis_sink_update_entry(
            self,
            "127.0.0.1",
            7000,
            "cluster",
            self.redis_cluster_container.get_client(),
        )

    def test_redis_sink_custom_key_cluster_mode(self):
        _test_redis_sink_custom_key(
            self,
            "127.0.0.1",
            7000,
            "cluster",
            self.redis_cluster_container.get_client(),
        )

    def test_redis_source_join_cluster_mode(self):
        _test_redis_source_join(
            self,
            "127.0.0.1",
            7000,
            "cluster",
            self.redis_cluster_container.get_client(),
        )

    def test_redis_source_join_different_key_order_cluster_mode(self):
        _test_redis_source_join_different_key_order(
            self,
            "127.0.0.1",
            7000,
            "cluster",
            self.redis_cluster_container.get_client(),
        )

    def _test_redis_source_join_custom_key_derived_feature_view_cluster_mode(
        self,
    ):
        _test_redis_source_join_custom_key_derived_feature_view(
            self,
            "127.0.0.1",
            7000,
            "cluster",
            self.redis_cluster_container.get_client(),
        )

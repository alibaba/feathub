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
from typing import Union

import pandas as pd
import redis
from redis import RedisCluster, Redis
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.redis import RedisContainer

from feathub.common import types
from feathub.common.types import Int64
from feathub.common.utils import (
    serialize_object_with_protobuf,
    to_unix_timestamp,
)
from feathub.feature_tables.sinks.redis_sink import RedisSink
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

    def start(self):
        super().start()
        self._wait_container_ready()
        return self


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

    sink = RedisSink(
        namespace="test_namespace",
        mode=mode,
        host=host,
        port=port,
    )

    self.client.materialize_features(
        features=source,
        sink=sink,
        allow_overwrite=True,
    ).wait(30000)

    if not isinstance(redis_client, RedisCluster):
        # Cluster client do not scan all nodes in the KEYS command
        self.assertEquals(len(redis_client.keys("*")), input_data.shape[0])

    for i in range(input_data.shape[0]):
        key = b"test_namespace:" + serialize_object_with_protobuf(
            input_data["id"][i], Int64
        )

        self.assertEquals(
            {
                int(0).to_bytes(4, byteorder="big"): serialize_object_with_protobuf(
                    i + 1, Int64
                ),
                b"__timestamp__": int(
                    to_unix_timestamp(
                        datetime(2022, 1, 1, 0, 0, i),
                        format="%Y-%m-%d %H:%M:%S",
                    )
                ).to_bytes(8, byteorder="big"),
            },
            redis_client.hgetall(key.decode("utf-8")),
        )

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

    def test_redis_sink_cluster_mode(self):
        _test_redis_sink(
            self,
            "127.0.0.1",
            7000,
            "cluster",
            self.redis_cluster_container.get_client(),
        )

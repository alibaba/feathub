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

import pandas as pd
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


class RedisSourceSinkITTest(ABC, FeathubITTestBase):
    redis_container = None

    def test_redis_sink(self):
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

        host, port, redis_client = self._get_redis_host_port_and_client()

        sink = RedisSink(
            namespace="test_namespace",
            host=host,
            port=port,
        )

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait(30000)

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

        self._tear_down_redis()

    @classmethod
    def _get_redis_host_port_and_client(cls):
        if cls.redis_container is None:
            cls.redis_container = RedisContainer()
            cls.redis_container.start()

        host = cls.redis_container.get_container_host_ip()
        if host == "localhost":
            host = "127.0.0.1"
        port = cls.redis_container.get_exposed_port(cls.redis_container.port_to_expose)
        client = cls.redis_container.get_client()
        return host, int(port), client

    @classmethod
    def _tear_down_redis(cls) -> None:
        if cls.redis_container is not None:
            cls.redis_container.stop()
            cls.redis_container = None

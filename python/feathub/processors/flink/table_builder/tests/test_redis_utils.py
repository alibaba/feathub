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
from unittest.mock import patch

from feathub.common.types import Int64, Unknown
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    DataTypes,
    TableDescriptor as NativeFlinkTableDescriptor,
)
from testcontainers.redis import RedisContainer

from feathub.common.utils import serialize_object_with_protobuf, to_unix_timestamp
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.processors.flink.table_builder.source_sink_utils import (
    insert_into_sink,
)
from feathub.processors.flink.table_builder.tests.table_builder_test_utils import (
    FlinkTableBuilderTestBase,
    MockTableDescriptor,
)
from feathub.table.table_descriptor import TableDescriptor


class SinkUtilTest(unittest.TestCase):
    def test_redis_sink(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)
        sink = RedisSink(
            namespace="test_namespace",
            host="127.0.0.1",
            port=6379,
            password="123456",
            db_num=3,
        )

        table = t_env.from_elements([(1,)]).alias("id")
        with patch.object(
            t_env, "create_temporary_table"
        ) as create_temporary_table, patch("pyflink.table.table.Table.execute_insert"):
            descriptor: TableDescriptor = MockTableDescriptor(keys=["id"])

            insert_into_sink(t_env, table, descriptor, sink)
            flink_table_descriptor: NativeFlinkTableDescriptor = (
                create_temporary_table.call_args[0][1]
            )

            expected_options = {
                "connector": "redis",
                "namespace": "test_namespace",
                "host": "127.0.0.1",
                "port": "6379",
                "password": "123456",
                "dbNum": "3",
                "keyField": "__redis_sink_key__",
            }
            self.assertEquals(
                expected_options, dict(flink_table_descriptor.get_options())
            )


class SourceSinkITTest(FlinkTableBuilderTestBase):
    redis_container: RedisContainer = None

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
        self.host = SourceSinkITTest.redis_container.get_container_host_ip()
        if self.host == "localhost":
            self.host = "127.0.0.1"
        self.port = SourceSinkITTest.redis_container.get_exposed_port(
            SourceSinkITTest.redis_container.port_to_expose
        )

        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.t_env = StreamTableEnvironment.create(self.env)

    def test_redis_sink(self):
        descriptor: TableDescriptor = MockTableDescriptor(
            keys=["id"], timestamp_field="ts", timestamp_format="%Y-%m-%d %H:%M:%S"
        )

        row_data = [
            (1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")),
            (2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")),
            (3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")),
            (4, None, datetime(2022, 1, 1, 0, 0, 3).strftime("%Y-%m-%d %H:%M:%S")),
        ]
        table = self.t_env.from_elements(
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
            namespace="test_namespace",
            host=self.host,
            port=int(self.port),
        )

        insert_into_sink(self.t_env, table, descriptor, sink).wait(30000)

        redis_client = self.redis_container.get_client()
        self.assertEquals(len(redis_client.keys("*")), len(row_data))

        for i in range(len(row_data)):
            key = b"test_namespace:" + serialize_object_with_protobuf(
                row_data[i][0], Int64
            )

            if i != 3:
                self.assertEquals(
                    {
                        int(0).to_bytes(
                            4, byteorder="big"
                        ): serialize_object_with_protobuf(i + 1, Int64),
                        b"__timestamp__": int(
                            to_unix_timestamp(
                                datetime(2022, 1, 1, 0, 0, i),
                                format="%Y-%m-%d %H:%M:%S",
                            )
                        ).to_bytes(8, byteorder="big"),
                    },
                    redis_client.hgetall(key.decode("utf-8")),
                )
            else:
                self.assertEquals(
                    {
                        int(0).to_bytes(
                            4, byteorder="big"
                        ): serialize_object_with_protobuf(None, Unknown),
                        b"__timestamp__": int(
                            to_unix_timestamp(
                                datetime(2022, 1, 1, 0, 0, i),
                                format="%Y-%m-%d %H:%M:%S",
                            )
                        ).to_bytes(8, byteorder="big"),
                    },
                    redis_client.hgetall(key.decode("utf-8")),
                )

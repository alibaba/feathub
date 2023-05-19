# Copyright 2022 The FeatHub Authors
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
from enum import Enum
from typing import List, Optional, Dict, Union

from feathub.common.exceptions import FeathubException
from feathub.common.utils import from_json, append_metadata_to_json
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema


class RedisMode(Enum):
    """
    Supported Redis deployment modes.
    """

    STANDALONE = "standalone"
    MASTER_SLAVE = "master_slave"
    CLUSTER = "cluster"


class RedisSource(FeatureTable):
    """
    A source which reads data from Redis. It can only read feature values written
    to Redis with :class:`RedisSink`.
    """

    def __init__(
        self,
        name: str,
        schema: Schema,
        keys: List[str],
        host: str,
        port: int = 6379,
        mode: Union[RedisMode, str] = RedisMode.STANDALONE,
        username: str = None,
        password: str = None,
        db_num: int = 0,
        namespace: str = "default",
        timestamp_field: Optional[str] = None,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param schema: The schema of the data.
        :param keys: The names of fields in this feature view that are necessary
                     to locate a row of this table.
        :param host: The host of the Redis instance to connect.
        :param port: The port of the Redis instance to connect.
        :param mode: The deployment mode or the name of the mode of the redis service.
        :param username: The username used by the Redis authorization process.
        :param password: The password used by the Redis authorization process.
        :param db_num: The No. of the Redis database to connect. Not supported in
                       Cluster mode.
        :param namespace: The namespace where the feature values reside in Redis. It
                          must be equal to the namespace of the corresponding RedisSink
                          when the features were written to Redis.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        """
        super().__init__(
            name=name,
            system_name="redis",
            table_uri={
                "host": host,
                "port": port,
                "db_num": db_num,
                "namespace": namespace,
            },
            keys=keys,
            schema=schema,
            timestamp_field=timestamp_field,
        )
        self.host = host
        self.schema = schema
        self.port = port
        self.mode = mode if isinstance(mode, RedisMode) else RedisMode(mode)
        self.username = username
        self.password = password
        self.db_num = db_num
        self.namespace = namespace

        if mode == RedisMode.CLUSTER and db_num != 0:
            raise FeathubException(
                "Selecting database is not supported in Cluster mode."
            )

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "schema": None if self.schema is None else self.schema.to_json(),
            "keys": self.keys,
            "host": self.host,
            "port": self.port,
            "mode": self.mode.name,
            "username": self.username,
            "password": self.password,
            "db_num": self.db_num,
            "namespace": self.namespace,
            "timestamp_field": self.timestamp_field,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "RedisSource":
        return RedisSource(
            name=json_dict["name"],
            schema=from_json(json_dict["schema"]),
            keys=json_dict["keys"],
            host=json_dict["host"],
            port=json_dict["port"],
            mode=json_dict["mode"],
            username=json_dict["username"],
            password=json_dict["password"],
            db_num=json_dict["db_num"],
            namespace=json_dict["namespace"],
            timestamp_field=json_dict["timestamp_field"],
        )

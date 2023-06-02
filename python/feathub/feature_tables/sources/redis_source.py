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


NAMESPACE_KEYWORD = "__NAMESPACE__"
KEYS_KEYWORD = "__KEYS__"
FEATURE_NAME_KEYWORD = "__FEATURE_NAME__"

KEY_COLUMN_PREFIX = "__KEY__"


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
        key_expr: str = 'CONCAT_WS(":", __NAMESPACE__, __KEYS__, __FEATURE_NAME__)',
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
        :param key_expr: A string that represents a FeatHub expression which evaluates
                         to a string value, which would be used as the key to a feature
                         saved in Redis. Apart from the field names, UDFs and other
                         grammars supported by Feathub expression, users may also use
                         the following keywords in this expression, which are
                         dynamically evaluated during compilation according to other
                         configurations or the structure of feature tables.
                         - __NAMESPACE__: the namespace to persist features in Redis.
                         - __KEYS__: A colon separated list of all key field names.
                         - __FEATURE_NAME__: the name of a feature to be written out
                           to Redis.
                         If not explicitly specified, the key would be a combination of
                         the namespace, all key field values, and the name of the
                         feature.
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
        self.key_expr = key_expr

        if NAMESPACE_KEYWORD not in key_expr:
            raise FeathubException(
                f"key_expr {key_expr} should contain {NAMESPACE_KEYWORD} in order "
                f"to guarantee the uniqueness of feature keys in Redis."
            )

        if FEATURE_NAME_KEYWORD not in key_expr:
            feature_names = [x for x in schema.field_names if x not in keys]
            if len(feature_names) > 1:
                raise FeathubException(
                    "In order to guarantee the uniqueness of feature keys in Redis, "
                    f"key_expr {key_expr} should contain {FEATURE_NAME_KEYWORD},"
                    f" or the input table should contain only one feature field."
                )

        if KEYS_KEYWORD not in key_expr and any(key not in key_expr for key in keys):
            raise FeathubException(
                f"key_expr {key_expr} does not contain {KEYS_KEYWORD} and all key "
                f"field names. Features saved to Redis might not have unique keys "
                f"and overwrite each other."
            )

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
            "mode": self.mode.value,
            "username": self.username,
            "password": self.password,
            "db_num": self.db_num,
            "namespace": self.namespace,
            "timestamp_field": self.timestamp_field,
            "key_expr": self.key_expr,
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
            key_expr=json_dict["key_expr"],
        )

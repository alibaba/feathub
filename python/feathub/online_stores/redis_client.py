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
import datetime
import json
from typing import Optional, List, Any, Dict
from typing import Union

import pandas as pd
import redis

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.types import MapType, VectorType
from feathub.feature_tables.sinks.redis_sink import RedisMode
from feathub.online_stores.online_store_client import OnlineStoreClient
from feathub.table.schema import Schema

REDIS_KEY_DELIMITER = ":"
ROW_KEY_DELIMITER = "-"


def _parse_bytes_or_string(data: Union[bytes, str], data_type: types.DType) -> Any:
    if data_type == types.Bytes:
        if isinstance(data, str):
            return data.encode("utf-8")
        return data

    if isinstance(data, bytes):
        data = data.decode("utf-8")

    if data_type == types.String:
        return data
    elif data_type == types.Bool:
        return data == "true"
    elif data_type == types.Int32:
        return int(data)
    elif data_type == types.Int64:
        return int(data)
    elif data_type == types.Float32:
        return float(data)
    elif data_type == types.Float64:
        return float(data)
    elif data_type == types.Timestamp:
        return datetime.datetime.fromtimestamp(int(data) / 1000.0)
    elif isinstance(data_type, types.VectorType):
        return _parse_list(json.loads(data), data_type)
    elif isinstance(data_type, types.MapType):
        return _parse_map(json.loads(data), data_type)

    raise FeathubException(f"Cannot read data with type {data_type} from Redis.")


def _parse_list(data: List[bytes], data_type: types.VectorType) -> List[Any]:
    result = []
    for element in data:
        result.append(_parse_bytes_or_string(element, data_type.dtype))
    return result


def _parse_map(data: Dict[bytes, bytes], data_type: types.MapType) -> Dict[Any, Any]:
    result = {}
    for key, value in data.items():
        result[
            _parse_bytes_or_string(key, data_type.key_dtype)
        ] = _parse_bytes_or_string(value, data_type.value_dtype)
    return result


class RedisClient(OnlineStoreClient):
    """
    An online store client that reads feature values from Redis.
    """

    def __init__(
        self,
        schema: Schema,
        host: str,
        port: int = 6379,
        mode: RedisMode = RedisMode.STANDALONE,
        username: str = None,
        password: str = None,
        db_num: int = 0,
        namespace: str = "default",
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
    ):
        super().__init__()
        self.namespace = namespace
        self.schema = schema
        self.key_names = keys
        self.key_types = [schema.get_field_type(x) for x in self.key_names]

        self.all_feature_names = [
            x
            for x in schema.field_names
            if x not in self.key_names and x != timestamp_field
        ]
        self.encoded_feature_indices = {}
        for i in range(len(self.all_feature_names)):
            self.encoded_feature_indices[self.all_feature_names[i]] = i.to_bytes(
                4, byteorder="big"
            )

        if mode == RedisMode.CLUSTER:
            self.redis_client: Union[
                redis.Redis, redis.RedisCluster
            ] = redis.RedisCluster(
                host=host,
                port=port,
                username=username,
                password=password,
                decode_responses=False,
            )
        else:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                username=username,
                password=password,
                db=db_num,
                decode_responses=False,
            )

    def get(
        self, input_data: pd.DataFrame, feature_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if not set(self.key_names) <= set(input_data.columns.values):
            raise RuntimeError(
                f"Input dataframe's column names {input_data.columns.values} "
                f"should contain all of source key field names {self.key_names}."
            )

        if feature_names is None:
            feature_names = self.all_feature_names

        results_list = []
        for _, row in input_data.iterrows():
            row_dict = row.to_dict()
            key_prefix = (
                self.namespace
                + REDIS_KEY_DELIMITER
                + ROW_KEY_DELIMITER.join(
                    str(row_dict[key_name]) for key_name in self.key_names
                )
            )

            result = []
            for feature_name in feature_names:
                key = key_prefix + REDIS_KEY_DELIMITER + feature_name
                field_type = self.schema.get_field_type(feature_name)
                if isinstance(field_type, MapType):
                    result.append(
                        _parse_map(self.redis_client.hgetall(key), field_type)
                    )
                elif isinstance(field_type, VectorType):
                    data: Any = _parse_list(
                        self.redis_client.lrange(key, 0, -1), field_type
                    )
                    result.append(data)
                else:
                    result.append(
                        _parse_bytes_or_string(self.redis_client.get(key), field_type)
                    )
            results_list.append(result)

        features = pd.DataFrame(results_list, columns=feature_names)
        features = input_data.join(features)
        return features

    def __del__(self) -> None:
        self.redis_client.close()

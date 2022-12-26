# Copyright 2022 The Feathub Authors
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
from typing import Optional, List

import pandas as pd
import redis

from feathub.common.utils import (
    deserialize_object_with_protobuf,
)
from feathub.online_stores.online_store_client import OnlineStoreClient
from feathub.processors.flink.table_builder.redis_utils import serialize_and_join_keys
from feathub.table.schema import Schema


class RedisClient(OnlineStoreClient):
    """
    An online store client that reads feature values from Redis.
    """

    def __init__(
        self,
        schema: Schema,
        host: str,
        port: int = 6379,
        username: str = None,
        password: str = None,
        db_num: int = 0,
        namespace: str = "default",
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
    ):
        super().__init__()
        self.namespace = namespace

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

        self.redis_client = redis.Redis(
            host=host,
            port=port,
            username=username,
            password=password,
            decode_responses=False,
        )
        self.redis_client.select(db_num)

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

        selected_feature_indices = [
            self.encoded_feature_indices[field] for field in feature_names
        ]

        results_list = []
        for _, row in input_data.iterrows():
            row_dict = row.to_dict()
            key_value = serialize_and_join_keys(
                [row_dict[x] for x in self.key_names], self.key_types
            )
            key_value = (self.namespace + ":").encode("utf-8") + key_value

            result = []
            raw_values = self.redis_client.hmget(key_value, selected_feature_indices)
            for raw_value in raw_values:
                result.append(deserialize_object_with_protobuf(bytes(raw_value)))
            results_list.append(result)

        features = pd.DataFrame(results_list, columns=feature_names)
        features = input_data.join(features)
        return features

    def __del__(self) -> None:
        self.redis_client.close()

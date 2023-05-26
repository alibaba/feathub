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
from typing import Union, Optional, List, Any

import pandas as pd
import redis

from feathub.common.types import MapType, VectorType
from feathub.dsl.expr_parser import ExprParser
from feathub.feature_tables.sinks.redis_sink import RedisMode
from feathub.feature_tables.sources.redis_source import (
    NAMESPACE_KEYWORD,
    KEYS_KEYWORD,
)
from feathub.online_stores.conversion_utils import to_python_object
from feathub.online_stores.online_store_client import OnlineStoreClient
from feathub.processors.local.ast_evaluator.local_ast_evaluator import LocalAstEvaluator
from feathub.table.schema import Schema


class RedisClient(OnlineStoreClient):
    """
    An online store client that reads feature values from Redis.
    """

    def __init__(
        self,
        schema: Schema,
        host: str,
        port: int,
        mode: RedisMode,
        username: str,
        password: str,
        db_num: int,
        namespace: str,
        keys: List[str],
        timestamp_field: str,
        key_expr: str,
    ):
        super().__init__()
        self.namespace = namespace
        self.schema = schema
        self.key_names = keys
        self.key_types = [schema.get_field_type(x) for x in self.key_names]

        self.key_expr_template = key_expr.replace(
            NAMESPACE_KEYWORD, f'"{namespace}"'
        ).replace(KEYS_KEYWORD, ", ".join(keys))

        self.parser = ExprParser()
        self.ast_evaluator = LocalAstEvaluator()

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
            result = []
            for feature_name in feature_names:
                key_expr = self.key_expr_template.replace(
                    "__FEATURE_NAME__", f'"{feature_name}"'
                )
                expr_node = self.parser.parse(key_expr)
                key = self.ast_evaluator.eval(expr_node, row_dict)

                field_type = self.schema.get_field_type(feature_name)
                if isinstance(field_type, MapType):
                    redis_data: Any = self.redis_client.hgetall(key)
                elif isinstance(field_type, VectorType):
                    redis_data = self.redis_client.lrange(key, 0, -1)
                else:
                    redis_data = self.redis_client.get(key)
                result.append(to_python_object(redis_data, field_type))
            results_list.append(result)

        features = pd.DataFrame(results_list, columns=feature_names)
        features = input_data.join(features)
        return features

    def __del__(self) -> None:
        self.redis_client.close()

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

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, List

import pandas as pd

from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sources.mysql_source import MySQLSource
from feathub.feature_tables.sources.redis_source import RedisSource


class OnlineStoreClient(ABC):
    """
    An online feature store client implements APIs to get features by keys. It can
    provide a uniform interface to interact with kv stores such as Redis.
    """

    def __init__(self) -> None:
        pass

    # TODO: replace input_data with keys.
    @abstractmethod
    def get(
        self, input_data: pd.DataFrame, feature_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Gets values matching the given keys from the specified table in the kv store.

        :param input_data: A DataFrame where each row contains the keys of this table.
        :param feature_names: Optional. The names of fields of values that should be
                               included in the output DataFrame. If it is None, all
                               feature fields of the specified table should be
                               outputted.
        :return: A DataFrame consisting of the input_data and the requested
                 feature_names.
        """
        pass

    @staticmethod
    def instantiate(source: FeatureTable) -> OnlineStoreClient:
        """
        Instantiates an OnlineStoreClient from the provided source.
        """

        if isinstance(source, RedisSource):
            from feathub.online_stores.redis_client import RedisClient

            return RedisClient(
                schema=source.schema,
                host=source.host,
                port=source.port,
                mode=source.mode,
                username=source.username,
                password=source.password,
                db_num=source.db_num,
                namespace=source.namespace,
                keys=source.keys,
                timestamp_field=source.timestamp_field,
                key_expr=source.key_expr,
            )

        if isinstance(source, MySQLSource):
            from feathub.online_stores.mysql_client import MySQLClient

            return MySQLClient(
                schema=source.schema,
                host=source.host,
                port=source.port,
                database=source.database,
                table=source.table,
                username=source.username,
                password=source.password,
                keys=source.keys,
                timestamp_field=source.timestamp_field,
            )

        raise RuntimeError(
            f"Failed to instantiate online store client from source {source}."
        )

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

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Optional, List

import pandas as pd

from feathub.online_stores.online_store_config import (
    OnlineStoreConfig,
    ONLINE_STORE_TYPES_CONFIG,
    OnlineStoreType,
)


def instantiate_online_stores(props: Dict) -> Dict[str, "OnlineStore"]:
    online_store_config = OnlineStoreConfig(props)
    store_types = online_store_config.get(ONLINE_STORE_TYPES_CONFIG)
    return {
        store_type: OnlineStore.instantiate(store_type, props)
        for store_type in store_types
    }


class OnlineStore(ABC):
    """
    An online feature store implements APIs to put and get features by keys. It can
    provide a uniform interface to interact with kv stores such as Redis.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def put(
        self,
        table_name: str,
        features: pd.DataFrame,
        key_fields: List[str],
        timestamp_field: Optional[str],
        timestamp_format: Optional[str],
    ) -> None:
        """
        For each row in the `features`, inserts or updates an entry of the specified
        table in the kv store. The key of each entry consists of values of the
        key_fields in the row. The value of this entry is the row itself.

        If timestamp_field is not None and an entry with the same key already exists,
        its value is updated only if the new row's timestamp > existing entry's
        timestamp.

        :param table_name: The table's name.
        :param features: A DataFrame with rows to put into the specified table.
        :param key_fields: The name of fields in the DataFrame to construct the key.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: Optional. The format of the timestamp field.
        """
        pass

    # TODO: replace input_data with keys.
    @abstractmethod
    def get(
        self,
        table_name: str,
        input_data: pd.DataFrame,
        feature_fields: Optional[List[str]] = None,
        include_timestamp_field: bool = False,
    ) -> pd.DataFrame:
        """
        Gets values matching the given keys from the specified table in the kv store.

        :param table_name: The name of the table containing the features.
        :param input_data: A DataFrame where each row contains the keys of this table.
        :param feature_fields: Optional. The names of fields of values that should be
                               included in the output DataFrame. If it is None, all
                               fields of the specified table should be outputted.
        :param include_timestamp_field: If it is true, the table should have a timestamp
                                        field. And the timestamp field will be outputted
                                        regardless of `feature_fields`.
        :return: A DataFrame consisting of the input_data and the requested
                 feature_fields.
        """
        pass

    @staticmethod
    def instantiate(store_type: str, props: Dict) -> OnlineStore:
        """
        Instantiates an OnlineStore of the given store type using the given properties.
        """

        online_store_type = OnlineStoreType(store_type)
        if online_store_type == OnlineStoreType.MEMORY:
            from feathub.online_stores.memory_online_store import MemoryOnlineStore

            return MemoryOnlineStore(props=props)

        raise RuntimeError(
            f"Failed to instantiate online store with type={store_type} and "
            f"props={props}."
        )

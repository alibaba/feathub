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
from typing import Optional, List, Any

import mysql.connector
import pandas as pd

from feathub.online_stores.online_store_client import OnlineStoreClient
from feathub.table.schema import Schema


class MySQLClient(OnlineStoreClient):
    def __init__(
        self,
        database: str,
        table: str,
        schema: Schema,
        host: str,
        port: int,
        username: str,
        password: str,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
    ):
        super().__init__()
        self.table = table
        self.keys = keys
        self.schema = schema
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        self.all_feature_names = [
            x for x in schema.field_names if x not in self.keys and x != timestamp_field
        ]

    def get(
        self, input_data: pd.DataFrame, feature_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if not set(self.keys) <= set(input_data.columns.values):
            raise RuntimeError(
                f"Input dataframe's column names {input_data.columns.values} "
                f"should contain all of source key field names {self.keys}."
            )

        if feature_names is None:
            feature_names = self.all_feature_names

        selected_field_names = [*self.keys, *feature_names]

        key_values = []
        for _, row in input_data.iterrows():
            row_dict = row.to_dict()
            key_values.append([row_dict[key] for key in self.keys])

        features = pd.DataFrame(
            data=self._query_rows_with_primary_key(selected_field_names, key_values),
            columns=selected_field_names,
        ).set_index(self.keys)
        features = input_data.join(features, on=self.keys)
        return features

    def _query_rows_with_primary_key(
        self, select_fields: List[str], key_values: List[List[Any]]
    ) -> List[Any]:
        conn = None
        cursor = None
        primary_keys_predicate = " AND ".join([f"`{key}`=%s" for key in self.keys])
        select_field_str = ",".join([f"`{f}`" for f in select_fields])
        sql = (
            f"SELECT {select_field_str} FROM {self.table} "
            f"WHERE {primary_keys_predicate}"
        )

        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
            )
            cursor = conn.cursor()

            results = []
            for key_value in key_values:
                cursor.execute(sql, key_value)

                result = cursor.fetchone()

                if result:
                    results.append(result)

            return results
        finally:
            if cursor is not None:
                cursor.close()

            if conn is not None:
                conn.close()

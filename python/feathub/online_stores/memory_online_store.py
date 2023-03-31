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

from collections import OrderedDict
from typing import Dict, List, Optional

import pandas as pd

import feathub.common.utils as utils
from feathub.common.exceptions import FeathubException
from feathub.common.types import to_numpy_dtype
from feathub.table.schema import Schema


class _TableInfo:
    def __init__(
        self,
        table: Dict,
        schema: Schema,
        timestamp_field: Optional[str],
        key_fields: List[str],
    ):
        self.table = table
        self.schema = schema
        self.timestamp_field = timestamp_field
        self.key_fields = key_fields


class MemoryOnlineStore:
    """
    An online store that stores all feature values in memory.
    """

    INSTANCE = None

    def __init__(self) -> None:
        super().__init__()
        self.table_infos: Dict[str, _TableInfo] = {}

    def put(
        self,
        table_name: str,
        features: pd.DataFrame,
        schema: Schema,
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
        :param schema: The FeatHub schema of the features.
        :param features: A DataFrame with rows to put into the specified table.
        :param key_fields: The name of fields in the DataFrame to construct the key.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: Optional. The format of the timestamp field.
        """

        if table_name not in self.table_infos:
            self.table_infos[table_name] = _TableInfo(
                table={},
                schema=schema,
                key_fields=key_fields,
                timestamp_field=timestamp_field,
            )
        table_info = self.table_infos[table_name]
        if table_info.schema != schema:
            raise RuntimeError(
                f"Features' dtypes {schema} do not match with dtypes "
                f"{table_info.schema} of the table {table_name}."
            )
        if table_info.timestamp_field != timestamp_field:
            raise RuntimeError(
                f"Features' field {timestamp_field} does not match with field "
                f"{table_info.timestamp_field} of the table {table_name}."
            )
        if not set(table_info.key_fields).issubset(list(features.columns)):
            raise RuntimeError(
                f"Features' columns {features.columns} do not have all "
                f"the keys {table_info.key_fields}."
            )

        table = table_info.table
        for index, row in features.iterrows():
            key = tuple(row.loc[key_fields].to_dict(OrderedDict).items())

            # overwrite if timestamp_field is None
            if timestamp_field is None:
                table[key] = row
                continue

            if timestamp_format is None:
                raise FeathubException(
                    "timestamp_format must not be None if timestamp_field is given."
                )
            time = utils.to_unix_timestamp(row[timestamp_field], timestamp_format)
            existing_row = table.get(key, None)
            if existing_row is not None:
                existing_time = utils.to_unix_timestamp(
                    existing_row[timestamp_field], timestamp_format
                )
                if existing_time >= time:
                    continue
            table[key] = row

    def get(
        self,
        table_name: str,
        input_data: pd.DataFrame,
        feature_names: Optional[List[str]] = None,
        include_timestamp_field: bool = False,
    ) -> pd.DataFrame:
        """
        Gets values matching the given keys from the specified table in the kv store.

        :param table_name: The name of the table containing the features.
        :param input_data: A DataFrame where each row contains the keys of this table.
        :param feature_names: Optional. The names of fields of values that should be
                               included in the output DataFrame. If it is None, all
                               fields of the specified table should be outputted.
        :param include_timestamp_field: If it is true, the table should have a timestamp
                                        field. And the timestamp field will be outputted
                                        regardless of `feature_names`.
        :return: A DataFrame consisting of the input_data and the requested
                 feature_names.
        """

        table_info = self.table_infos[table_name]
        table = table_info.table
        key_fields = table_info.key_fields
        if not set(key_fields).issubset(list(input_data.columns)):
            raise RuntimeError(f"Input data does not have all the keys {key_fields}.")
        field_to_drop = table_info.timestamp_field
        if include_timestamp_field:
            field_to_drop = None

        rows = []
        for index, row in input_data.iterrows():
            key = tuple(row.loc[key_fields].to_dict(OrderedDict).items())
            row = table[key]
            rows.append(row)

        schema = self.table_infos[table_name].schema
        features = (
            pd.concat(rows, axis=1)
            .transpose()
            .reset_index(drop=True)
            .astype(
                {
                    field_name: to_numpy_dtype(schema.get_field_type(field_name))
                    for field_name in schema.field_names
                }
            )
        )
        features = features.drop(columns=key_fields)
        input_data = input_data.drop(columns=features.columns.tolist(), errors="ignore")
        features = input_data.join(features)

        if feature_names is not None:
            if table_info.timestamp_field is not None:
                feature_names.append(table_info.timestamp_field)
            features = features[input_data.columns.values.tolist() + feature_names]
        if table_info.timestamp_field is not None and not include_timestamp_field:
            features = features.drop(columns=[field_to_drop])
        return features

    def reset(self) -> None:
        self.table_infos = {}

    @staticmethod
    def get_instance() -> MemoryOnlineStore:
        if MemoryOnlineStore.INSTANCE is None:
            MemoryOnlineStore.INSTANCE = MemoryOnlineStore()

        return MemoryOnlineStore.INSTANCE

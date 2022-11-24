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

from typing import Dict, List, Optional, Mapping
from collections import OrderedDict
import pandas as pd

import feathub.common.utils as utils
from feathub.common.exceptions import FeathubException
from feathub.online_stores.online_store import OnlineStore


class _TableInfo:
    def __init__(
        self,
        table: Dict,
        dtypes: Mapping,
        timestamp_field: Optional[str],
        key_fields: List[str],
    ):
        self.table = table
        self.dtypes = dtypes
        self.timestamp_field = timestamp_field
        self.key_fields = key_fields


class MemoryOnlineStore(OnlineStore):
    """
    An online store that stores all feature values in memory.
    """

    STORE_TYPE = "memory"

    def __init__(self, props: Dict) -> None:
        """
        :param props: The store properties.
        """
        super().__init__()
        self.table_infos: Dict[str, _TableInfo] = {}

    def put(
        self,
        table_name: str,
        features: pd.DataFrame,
        key_fields: List[str],
        timestamp_field: Optional[str],
        timestamp_format: Optional[str],
    ) -> None:
        if table_name not in self.table_infos:
            self.table_infos[table_name] = _TableInfo(
                table={},
                dtypes=features.dtypes.to_dict(),
                key_fields=key_fields,
                timestamp_field=timestamp_field,
            )
        table_info = self.table_infos[table_name]
        dtypes = features.dtypes.to_dict()
        if table_info.dtypes != dtypes:
            raise RuntimeError(
                f"Features' dtypes {dtypes} do not match with table {table_name}."
            )
        if table_info.timestamp_field != timestamp_field:
            raise RuntimeError(
                f"Features' {timestamp_field} does not match with table {table_name}."
            )
        if not set(table_info.key_fields).issubset(list(features.columns)):
            raise RuntimeError("Features do not have all the keys.")

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
        feature_fields: Optional[List[str]] = None,
        include_timestamp_field: bool = False,
    ) -> pd.DataFrame:
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

        # TODO: move this logic to FeatureService.
        features = (
            pd.concat(rows, axis=1)
            .transpose()
            .reset_index(drop=True)
            .astype(self.table_infos[table_name].dtypes)
        )
        features = features.drop(columns=key_fields)
        input_data = input_data.drop(columns=features.columns.tolist(), errors="ignore")
        features = input_data.join(features)

        if feature_fields is not None:
            if table_info.timestamp_field is not None:
                feature_fields.append(table_info.timestamp_field)
            features = features[input_data.columns.values.tolist() + feature_fields]
        if table_info.timestamp_field is not None and not include_timestamp_field:
            features = features.drop(columns=[field_to_drop])
        return features

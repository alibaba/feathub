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

import unittest
import tempfile
import shutil
import pandas as pd
from typing import Optional, List

from feathub.common.types import from_numpy_dtype
from feathub.processors.local.local_processor import LocalProcessor
from feathub.registries.local_registry import LocalRegistry
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sinks.online_store_sink import OnlineStoreSink
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.online_stores.online_store import OnlineStore
from feathub.table.schema import Schema


class LocalProcessorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        memory_online_store = OnlineStore.instantiate(
            store_type=MemoryOnlineStore.STORE_TYPE, props={}
        )
        self.stores = {MemoryOnlineStore.STORE_TYPE: memory_online_store}
        self.registry = LocalRegistry(props={})
        self.processor = LocalProcessor(
            props={}, stores=self.stores, registry=self.registry
        )
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        timestamp_field: str = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> FileSystemSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        schema = Schema(
            field_names=df.keys().tolist(),
            field_types=[from_numpy_dtype(dtype) for dtype in df.dtypes],
        )
        df.to_csv(path, index=False, header=False)

        return FileSystemSource(
            name="source",
            path=path,
            data_format="csv",
            schema=schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    def _materialize_and_get_online_features(
        self,
        table_name: str,
        input_data: pd.DataFrame,
        keys_to_get: pd.DataFrame,
        feature_fields: Optional[List[str]] = None,
        include_timestamp_field: bool = True,
    ) -> pd.DataFrame:
        sink = OnlineStoreSink(
            store_type=MemoryOnlineStore.STORE_TYPE,
            table_name=table_name,
        )

        source = self._create_file_source(input_data, keys=["name"])
        self.processor.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        return self.processor.get_online_features(
            table_name=table_name,
            input_data=keys_to_get,
            feature_fields=feature_fields,
            include_timestamp_field=include_timestamp_field,
            store_type=MemoryOnlineStore.STORE_TYPE,
        )

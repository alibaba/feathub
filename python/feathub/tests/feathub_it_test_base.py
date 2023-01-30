#  Copyright 2022 The Feathub Authors
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
import shutil
import tempfile
import unittest
import uuid
from abc import abstractmethod
from typing import Optional, List, Dict, Type
from unittest import TestLoader

import pandas as pd

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.table.schema import Schema


def _merge_nested_dict(a, b) -> None:
    """
    Merges dict b into dict a. a and b might be nested dict.
    """
    for key in b:
        if key not in a:
            a[key] = b[key]
        elif isinstance(a[key], dict) and isinstance(b[key], dict):
            _merge_nested_dict(a[key], b[key])
        elif a[key] != b[key]:
            raise FeathubException(
                f"Mismatch value {a[key]} and {b[key]} found for key {key}"
            )


class FeathubITTestBase(unittest.TestCase):
    """
    Abstract base class for all Feathub integration tests. A child class of
    this class must instantiate the corresponding FeathubClient instance and
    have its test cases use Feathub public APIs to get and write features.

    This class also provides utility variables and methods to assist the construction
    of test cases.
    """

    # By setting this attribute to false, it prevents pytest from discovering
    # this class as a test when searching up from its child classes.
    __test__ = False

    # A dict holding the base test class for each inherited test method.
    _base_class_mapping: Dict[str, Type[unittest.TestCase]] = None

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.input_data, self.schema = self.create_input_data_and_schema()
        self.client = self.get_client()

    def tearDown(self) -> None:
        MemoryOnlineStore.get_instance().reset()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @abstractmethod
    def get_client(self, extra_config: Optional[Dict] = None) -> FeathubClient:
        """
        Returns a FeathubClient instance for test cases.
        """
        pass

    @staticmethod
    def get_client_with_local_registry(
        processor_config: Dict, extra_config: Optional[Dict] = None
    ) -> FeathubClient:
        props = {
            "processor": processor_config,
            "online_store": {
                "types": ["memory"],
                "memory": {},
            },
            "registry": {
                "type": "local",
                "local": {
                    "namespace": "default",
                },
            },
            "feature_service": {
                "type": "local",
                "local": {},
            },
        }

        if extra_config is not None:
            _merge_nested_dict(props, extra_config)

        return FeathubClient(props)

    def create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        schema: Optional[Schema] = None,
        timestamp_field: Optional[str] = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
        name: str = None,
    ) -> FileSystemSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        if schema is None:
            schema = self._create_input_schema()
        df.to_csv(path, index=False, header=False)

        if name is None:
            name = self.generate_random_name("source")

        return FileSystemSource(
            name=name,
            path=path,
            data_format="csv",
            schema=schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    # TODO: only invoke the corresponding base class's setUpClass()
    #  method to reduce resource consumption.
    @classmethod
    def invoke_all_base_class_setupclass(cls):
        for base_class in cls.__bases__:
            if issubclass(base_class, unittest.TestCase):
                base_class.setUpClass()

    @classmethod
    def invoke_all_base_class_teardownclass(cls):
        for base_class in cls.__bases__:
            if issubclass(base_class, unittest.TestCase):
                base_class.tearDownClass()

    def invoke_base_class_setup(self):
        self._get_base_test_class().setUp(self)

    def invoke_base_class_teardown(self):
        self._get_base_test_class().tearDown(self)

    def _get_base_test_class(self) -> Type[unittest.TestCase]:
        if self._base_class_mapping is None:
            self._base_class_mapping = dict()
            for base_class in self.__class__.__bases__:
                if not issubclass(base_class, unittest.TestCase):
                    continue
                for func in dir(base_class):
                    if not (
                        callable(getattr(base_class, func))
                        and func.startswith(TestLoader.testMethodPrefix)
                    ):
                        continue
                    if func in self._base_class_mapping:
                        raise FeathubException(
                            f"Duplicated test case name {func} detected in integration "
                            f"test base class {self._base_class_mapping[func]} and "
                            f"{base_class}."
                        )
                    self._base_class_mapping[func] = base_class
        if self._testMethodName in self._base_class_mapping:
            return self._base_class_mapping[self._testMethodName]
        return FeathubITTestBase

    @classmethod
    def generate_random_name(cls, root_name: str) -> str:
        random_name = f"{root_name}_{str(uuid.uuid4()).replace('-', '')}"
        return random_name

    @classmethod
    def create_input_data_and_schema(cls):
        input_data = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        schema = cls._create_input_schema()

        return input_data, schema

    @classmethod
    def create_input_data_and_schema_with_millis_time_span(cls):
        input_data = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:00:00.001"],
                ["Emma", 400, 250, "2022-01-01 08:00:00.002"],
                ["Alex", 300, 200, "2022-01-01 08:00:00.003"],
                ["Emma", 200, 250, "2022-01-01 08:00:00.004"],
                ["Jack", 500, 500, "2022-01-01 08:00:00.005"],
                ["Alex", 600, 800, "2022-01-01 08:00:00.006"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        schema = cls._create_input_schema()

        return input_data, schema

    @classmethod
    def _create_input_schema(cls):
        return (
            Schema.new_builder()
            .column("name", types.String)
            .column("cost", types.Int64)
            .column("distance", types.Int64)
            .column("time", types.String)
            .build()
        )

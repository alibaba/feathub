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
from typing import Any

from testcontainers.mysql import MySqlContainer

from feathub.feathub_client import FeathubClient
from feathub.registries.tests.test_registry import RegistryTestBase


class MySqlRegistryTest(RegistryTestBase):
    __test__ = True

    mysql_container = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.mysql_container = MySqlContainer(image="mysql:8.0")
        cls.mysql_container.start()
        cls.client = FeathubClient(
            {
                "processor": {
                    "type": "local",
                },
                "online_store": {
                    "types": ["memory"],
                    "memory": {},
                },
                "registry": {
                    "type": "mysql",
                    "mysql": {
                        "database": cls.mysql_container.MYSQL_DATABASE,
                        "table": "feathub_registry_features",
                        "host": cls.mysql_container.get_container_host_ip(),
                        "port": int(
                            cls.mysql_container.get_exposed_port(
                                cls.mysql_container.port_to_expose
                            )
                        ),
                        "username": cls.mysql_container.MYSQL_USER,
                        "password": cls.mysql_container.MYSQL_PASSWORD,
                    },
                },
                "feature_service": {
                    "type": "local",
                    "local": {},
                },
            }
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.mysql_container.stop()
        cls.mysql_container = None

    def tearDown(self) -> None:
        super().tearDown()
        registry: Any = self.client.registry
        registry.cursor.execute("DELETE FROM feathub_registry_features;")

    def _get_client(self) -> FeathubClient:
        return self.client

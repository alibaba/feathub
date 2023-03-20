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
from typing import Dict, Optional

from feathub.feature_tables.sinks.sink import Sink


class MySQLSink(Sink):
    """A Sink that writes data to a MySQL table."""

    def __init__(
        self,
        host: str,
        database: str,
        table: str,
        username: str,
        password: str,
        port: int = 3306,
        extra_config: Optional[Dict[str, str]] = None,
    ):
        """
        :param host: IP address or hostname of the MySQL server.
        :param database: Database name to write to.
        :param table: Table name of the table to write to.
        :param username: Name of the user to connect to the MySQL server.
        :param password: The password of the user.
        :param port: The port of the MySQL server.
        :param extra_config: Extra configurations to be passthrough to the processor.
                             The available configurations are different for different
                             processors.
        """
        super().__init__(
            name="",
            system_name="mysql",
            properties={
                "host": host,
                "port": port,
                "database": database,
                "table": table,
            },
        )

        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.username = username
        self.password = password
        self.extra_config = {} if extra_config is None else extra_config

    def to_json(self) -> Dict:
        return {
            "type": "MySQLSink",
            "host": self.host,
            "database": self.database,
            "table": self.table,
            "username": self.username,
            "password": self.password,
            "port": self.port,
            "extra_config": self.extra_config,
        }

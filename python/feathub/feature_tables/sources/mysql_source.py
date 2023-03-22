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
from typing import Dict, List, Optional

from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema


# TODO: Support using MySQLSource as source other than OnDemandFeatureView.
class MySQLSource(FeatureTable):
    """
    A source which reads data from MySQL. Currently, MySQLSource can only be use as
    source of OnDemandFeatureView to get online feature from MySQL.
    """

    def __init__(
        self,
        name: str,
        database: str,
        table: str,
        schema: Schema,
        host: str,
        username: str,
        password: str,
        port: int = 3306,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        extra_config: Optional[Dict[str, str]] = None,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param database: Database name to write to.
        :param table: Table name of the table to write to.
        :param schema: The schema of the table.
        :param host: IP address or hostname of the MySQL server.
        :param username: Name of the user to connect to the MySQL server.
        :param password: The password of the user.
        :param port: The port of the MySQL server.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field.
        :param extra_config: Extra configurations to be passthrough to the processor.
                             The available configurations are different for different
                             processors.
        """
        super().__init__(
            name=name,
            system_name="mysql",
            properties={
                "host": host,
                "port": port,
                "database": database,
                "table": table,
            },
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            schema=schema,
        )

        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.username = username
        self.password = password
        self.extra_config = extra_config

    def to_json(self) -> Dict:
        return {
            "type": "MySQLSource",
            "name": self.name,
            "database": self.database,
            "table": self.table,
            "schema": self.schema,
            "host": self.host,
            "username": self.username,
            "password": self.password,
            "port": self.port,
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "extra_config": self.extra_config,
        }

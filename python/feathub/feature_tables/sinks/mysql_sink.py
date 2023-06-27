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
from typing import Dict, Optional

from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.sinks.sink import Sink


class MySQLSink(Sink):
    """A Sink that writes data to a MySQL table."""

    def __init__(
        self,
        database: str,
        table: str,
        host: str,
        username: str,
        password: str,
        port: int = 3306,
        processor_specific_props: Optional[Dict[str, str]] = None,
        keep_timestamp_field: bool = True,
    ):
        """
        :param database: Database name to write to.
        :param table: Table name of the table to write to.
        :param host: IP address or hostname of the MySQL server.
        :param username: Name of the user to connect to the MySQL server.
        :param password: The password of the user.
        :param port: The port of the MySQL server.
        :param processor_specific_props: Extra properties to be passthrough to the
                                         processor. The available configurations are
                                         different for different processors.
        :param keep_timestamp_field: True if the timestamp field of the feature table
                                     should be persisted to the external system through
                                     the sink.
        """
        super().__init__(
            name="",
            system_name="mysql",
            table_uri={
                "host": host,
                "port": port,
                "database": database,
                "table": table,
            },
            keep_timestamp_field=keep_timestamp_field,
        )

        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.username = username
        self.password = password
        self.processor_specific_props = (
            {} if processor_specific_props is None else processor_specific_props
        )

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "database": self.database,
            "table": self.table,
            "host": self.host,
            "username": self.username,
            "password": self.password,
            "port": self.port,
            "processor_specific_props": self.processor_specific_props,
            "keep_timestamp_field": self.keep_timestamp_field,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "MySQLSink":
        return MySQLSink(
            database=json_dict["database"],
            table=json_dict["table"],
            host=json_dict["host"],
            username=json_dict["username"],
            password=json_dict["password"],
            port=json_dict["port"],
            processor_specific_props=json_dict["processor_specific_props"],
            keep_timestamp_field=json_dict["keep_timestamp_field"],
        )

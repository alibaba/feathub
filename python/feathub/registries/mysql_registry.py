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
import json
from datetime import datetime
from hashlib import sha256
from typing import List, Optional, Dict, Any, Tuple

import mysql.connector

from feathub.common.config import ConfigDef
from feathub.common.exceptions import FeathubException
from feathub.common.utils import from_json
from feathub.registries.registry import Registry
from feathub.registries.registry_config import REGISTRY_PREFIX, RegistryConfig
from feathub.table.table_descriptor import TableDescriptor

MYSQL_REGISTRY_PREFIX = REGISTRY_PREFIX + "mysql."

DATABASE_CONFIG = MYSQL_REGISTRY_PREFIX + "database"
DATABASE_DOC = "The name of the MySQL database to hold the Feathub registry."

TABLE_CONFIG = MYSQL_REGISTRY_PREFIX + "table"
TABLE_DOC = "The name of the MySQL table to hold the Feathub registry."

HOST_CONFIG = MYSQL_REGISTRY_PREFIX + "host"
HOST_DOC = "IP address or hostname of the MySQL server."

PORT_CONFIG = MYSQL_REGISTRY_PREFIX + "port"
PORT_DOC = "The port of the MySQL server."

USERNAME_CONFIG = MYSQL_REGISTRY_PREFIX + "username"
USERNAME_DOC = "Name of the user to connect to the MySQL server."

PASSWORD_CONFIG = MYSQL_REGISTRY_PREFIX + "password"
PASSWORD_DOC = "The password of the user."


# TODO: validate and throw exception if a required config's value is NONE.
mysql_registry_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=DATABASE_CONFIG,
        value_type=str,
        description=DATABASE_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=TABLE_CONFIG,
        value_type=str,
        description=TABLE_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=HOST_CONFIG,
        value_type=str,
        description=HOST_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=PORT_CONFIG,
        value_type=int,
        description=PORT_DOC,
        default_value=3306,
    ),
    ConfigDef(
        name=USERNAME_CONFIG,
        value_type=str,
        description=USERNAME_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=PASSWORD_CONFIG,
        value_type=str,
        description=PASSWORD_DOC,
        default_value=None,
    ),
]


class MySqlRegistryConfig(RegistryConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(mysql_registry_config_defs)


def _get_digest(original_descriptor: str, resolved_descriptor: str) -> str:
    return sha256(
        (original_descriptor + resolved_descriptor).encode("utf-8")
    ).hexdigest()


class MySqlRegistry(Registry):
    """
    A registry that stores entities in a MySQL database.
    """

    def __init__(
        self,
        props: Dict,
    ):
        super().__init__("mysql", props)
        mysql_registry_config = MySqlRegistryConfig(props)
        self.database = mysql_registry_config.get(DATABASE_CONFIG)
        self.table = mysql_registry_config.get(TABLE_CONFIG)
        self.host = mysql_registry_config.get(HOST_CONFIG)
        self.port = mysql_registry_config.get(PORT_CONFIG)
        self.username = mysql_registry_config.get(USERNAME_CONFIG)
        self.password = mysql_registry_config.get(PASSWORD_CONFIG)

        # dict that acts as a local cache for built and registered descriptors.
        # each value in the dict is a tuple of
        # - original descriptor.
        # - resolved descriptor.
        # - UTC timestamp when the descriptor is resolved.
        self.tables: Dict[str, Tuple[TableDescriptor, TableDescriptor, datetime]] = {}

        self.conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
        )
        self.cursor = self.conn.cursor()

        self.cursor.execute(
            f"""
                CREATE TABLE IF NOT EXISTS `{self.table}`(
                   `name` VARCHAR(64) NOT NULL,
                   `timestamp` TIMESTAMP NOT NULL,
                   `digest` VARCHAR(64) NOT NULL,
                   `original_descriptor` TEXT NOT NULL,
                   `resolved_descriptor` TEXT NOT NULL,
                   PRIMARY KEY ( `name`, `timestamp` )
                );
            """
        )

    def build_features(
        self,
        feature_descriptors: List[TableDescriptor],
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> List[TableDescriptor]:
        result = []
        for table in feature_descriptors:
            if table.name == "":
                raise FeathubException(
                    "Cannot build a TableDescriptor with empty name."
                )

            # TODO: add document about the limitations on the length of feature
            #  table names.
            if len(table.name) > 64:
                raise FeathubException(
                    "Cannot build or register a descriptor with a name longer"
                    "than 64 characters."
                )

            self.tables[table.name] = (
                table,
                table.build(self, force_update=force_update, props=props),
                datetime.utcnow(),
            )
            result.append(self.tables[table.name][1])

        return result

    def register_features(
        self, feature_descriptors: List[TableDescriptor], force_update: bool = False
    ) -> List[bool]:
        self.build_features(feature_descriptors, force_update=force_update)

        results = []
        for descriptor in feature_descriptors:
            existing_descriptor = self._get_descriptor_from_mysql(descriptor.name)

            original_descriptor = json.dumps(
                self.tables[descriptor.name][0].to_json(), sort_keys=True
            )
            resolved_descriptor = json.dumps(
                self.tables[descriptor.name][1].to_json(), sort_keys=True
            )
            digest = _get_digest(original_descriptor, resolved_descriptor)

            if existing_descriptor is not None and (
                existing_descriptor[1] == digest
                or existing_descriptor[0] >= self.tables[descriptor.name][2]
            ):
                results.append(False)
                continue

            original_descriptor = original_descriptor.replace('"', '\\"')
            resolved_descriptor = resolved_descriptor.replace('"', '\\"')
            timestamp = self.tables[descriptor.name][2].strftime("%Y-%m-%d %H:%M:%S %z")
            self.cursor.execute(
                f"""
                    INSERT INTO {self.table} (
                       `name`,
                       `timestamp`,
                       `digest`,
                       `original_descriptor`,
                       `resolved_descriptor`
                    ) VALUES (
                        "{descriptor.name}",
                        "{timestamp}",
                        "{digest}",
                        "{original_descriptor}",
                        "{resolved_descriptor}"
                    );
                """
            )
            results.append(True)
        return results

    def get_features(
        self, name: str, force_update: bool = False, is_resolved: bool = True
    ) -> TableDescriptor:
        if force_update:
            self._get_descriptor_from_mysql(name)

        if name not in self.tables:
            raise RuntimeError(
                f"Table '{name}' is not found in the cache or registry. "
                "Please invoke build_features(..) for this table."
            )

        return self.tables[name][1 if is_resolved else 0]  # type: ignore

    def delete_features(self, name: str) -> bool:
        if self._get_descriptor_from_mysql(name) is None and name not in self.tables:
            return False

        self.cursor.execute(
            f"""
                DELETE FROM {self.table}
                WHERE `name` = "{name}";
            """
        )
        self.tables.pop(name)
        return True

    def _get_descriptor_from_mysql(
        self, name: str
    ) -> Optional[Tuple[datetime, str, TableDescriptor, TableDescriptor]]:
        self.cursor.execute(
            f"""
                SELECT
                    `timestamp`,
                    `digest`,
                    `original_descriptor`,
                    `resolved_descriptor`
                FROM {self.table}
                WHERE `name` = "{name}"
                ORDER BY `timestamp` DESC
                LIMIT 1;
            """
        )
        results: List[Tuple] = self.cursor.fetchall()
        if len(results) == 0:
            return None

        timestamp, digest, original_descriptor, resolved_descriptor = results[0]
        if _get_digest(original_descriptor, resolved_descriptor) != digest:
            raise FeathubException(
                f"Acquired features' json string cannot match the digest. "
                f"Data might be broken. Json string: {resolved_descriptor}, "
                f"digest: {digest}"
            )

        original_descriptor = from_json(json.loads(original_descriptor))
        resolved_descriptor = from_json(json.loads(resolved_descriptor))

        if name not in self.tables or self.tables[name][2] < timestamp:
            self.tables[name] = (original_descriptor, resolved_descriptor, timestamp)

        return timestamp, digest, original_descriptor, resolved_descriptor

    def __del__(self) -> None:
        self.cursor.close()
        self.conn.close()

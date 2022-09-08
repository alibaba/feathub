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
from datetime import timedelta
from typing import List, Optional, Dict

from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema


class FileSystemSource(FeatureTable):
    """A source which reads data from files."""

    def __init__(
        self,
        name: str,
        path: str,
        data_format: str,
        schema: Schema,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        max_out_of_orderness: timedelta = timedelta(0),
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param path: The path to files.
        :param data_format: The format that should be used to read files.
        :param schema: The schema of the data.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field.
        :param max_out_of_orderness: The maximum amount of time a record is allowed to
                                     be late. Default is 0 second, meaning the records
                                     should be ordered by `timestamp_field`.
        """
        super().__init__(
            name=name,
            system_name="filesystem",
            properties={
                "path": path,
            },
            data_format=data_format,
            keys=keys,
            schema=schema,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.path = path
        self.schema = schema
        self.max_out_of_orderness = max_out_of_orderness

    def to_json(self) -> Dict:
        return {
            "type": "FileSystemSource",
            "name": self.name,
            "path": self.path,
            "data_format": self.data_format,
            "schema": None if self.schema is None else self.schema.to_json(),
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "max_out_of_orderness_ms": self.max_out_of_orderness
            / timedelta(milliseconds=1),
        }

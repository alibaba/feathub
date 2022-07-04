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

from typing import List, Optional, Dict

from feathub.sources.source import Source
from feathub.table.schema import Schema


class FileSource(Source):
    """A source which reads data from files."""

    def __init__(
        self,
        name: str,
        path: str,
        file_format: str,
        schema: Schema,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param path: The path to files.
        :param file_format: The format that should be used to read files.
        :param schema: The schema of the data.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        """
        super().__init__(
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.path = path
        self.file_format = file_format
        self.schema = schema

    def to_json(self) -> Dict:
        return {
            "type": "FileSource",
            "name": self.name,
            "path": self.path,
            "file_format": self.file_format,
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "schema": None if self.schema is None else self.schema.to_json(),
        }

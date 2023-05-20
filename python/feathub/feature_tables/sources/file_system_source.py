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
from datetime import timedelta
from typing import List, Optional, Dict, Any

from feathub.common.utils import from_json, append_metadata_to_json
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
        data_format_props: Optional[Dict[str, Any]] = None,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param path: The path to a file or a directory of files to read. Note that there
                     is no defined order of ingestion for the files inside the
                     directory.
        :param data_format: The format that should be used to read files.
        :param schema: The schema of the data.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field. See TableDescriptor
                                 for valid format values. Only effective when the
                                 `timestamp_field` is not None.
        :param max_out_of_orderness: The maximum amount of time a record is allowed to
                                     be late. Default is 0 second, meaning the records
                                     should be ordered by `timestamp_field`.
        :param data_format_props: The properties of the data format.
        """
        super().__init__(
            name=name,
            system_name="filesystem",
            table_uri={
                "path": path,
            },
            data_format=data_format,
            keys=keys,
            schema=schema,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            data_format_props=data_format_props,
        )
        self.path = path
        self.schema = schema
        self.max_out_of_orderness = max_out_of_orderness

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "path": self.path,
            "data_format": self.data_format,
            "schema": None if self.schema is None else self.schema.to_json(),
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "max_out_of_orderness_ms": self.max_out_of_orderness
            / timedelta(milliseconds=1),
            "data_format_props": self.data_format_props,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "FileSystemSource":
        return FileSystemSource(
            name=json_dict["name"],
            path=json_dict["path"],
            data_format=json_dict["data_format"],
            schema=from_json(json_dict["schema"])
            if json_dict["schema"] is not None
            else None,
            keys=json_dict["keys"],
            timestamp_field=json_dict["timestamp_field"],
            timestamp_format=json_dict["timestamp_format"],
            max_out_of_orderness=timedelta(
                milliseconds=json_dict["max_out_of_orderness_ms"]
            ),
            data_format_props=json_dict["data_format_props"],
        )

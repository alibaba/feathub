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
from typing import Dict, Optional, Any

from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.sinks.sink import Sink


class FileSystemSink(Sink):
    """A Sink which writes data to files."""

    def __init__(
        self,
        path: str,
        data_format: str,
        data_format_props: Optional[Dict[str, Any]] = None,
        keep_timestamp_field: bool = True,
    ) -> None:
        """
        :param path: The path to directory of files to write to.
        :param data_format: The format of the data that are written to the file.
        :param data_format_props: The properties of the data format.
        :param keep_timestamp_field: True if the timestamp field of the feature table
                                     should be persisted to the external system through
                                     the sink.
        """
        super().__init__(
            name="",
            system_name="filesystem",
            table_uri={"path": path},
            data_format=data_format,
            data_format_props=data_format_props,
            keep_timestamp_field=keep_timestamp_field,
        )
        self.path = path

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "path": self.path,
            "data_format": self.data_format,
            "data_format_props": self.data_format_props,
            "keep_timestamp_field": self.keep_timestamp_field,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "FileSystemSink":
        return FileSystemSink(
            path=json_dict["path"],
            data_format=json_dict["data_format"],
            data_format_props=json_dict["data_format_props"],
            keep_timestamp_field=json_dict["keep_timestamp_field"],
        )

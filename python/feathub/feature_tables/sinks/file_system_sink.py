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
from typing import Dict

from feathub.feature_tables.sinks.sink import Sink


class FileSystemSink(Sink):
    """A Sink which writes data to files."""

    def __init__(self, path: str, data_format: str) -> None:
        """
        :param path: The path to files.
        :param data_format: The format of the data that are written to the file.
        """
        super().__init__(
            name="",
            system_name="filesystem",
            properties={"path": path},
            data_format=data_format,
        )
        self.path = path

    def to_json(self) -> Dict:
        return {
            "type": "FileSystemSink",
            "path": self.path,
            "data_format": self.data_format,
        }

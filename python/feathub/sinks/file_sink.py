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

from feathub.sinks.sink import Sink


class FileSink(Sink):
    def __init__(self, path: str, file_format: str, allow_overwrite: bool) -> None:
        super().__init__("filesystem", allow_overwrite)
        self.path = path
        self.file_format = file_format

    def to_json(self) -> Dict:
        return {
            "path": self.path,
            "file_format": self.file_format,
            "allow_overwrite": self.allow_overwrite,
        }

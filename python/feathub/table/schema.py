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

from typing import List
import json

from feathub.common.types import DType


class Schema:
    """Schema of a table."""

    def __init__(self, field_names: List[str], field_types: List[DType]):
        """
        :param field_names: Names of table's columns.
        :param field_types: Data types of table's columns.
        """
        self.field_names = field_names
        self.field_types = field_types

    def __str__(self):
        values = [
            (i, self.field_names[i], self.field_types[i].to_json())
            for i in range(len(self.field_names))
        ]

        return json.dumps(values, indent=2, sort_keys=True)

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

import json
from typing import List, Dict

from feathub.common.exceptions import FeathubException
from feathub.common.types import DType


class Schema:
    """Schema of a table."""

    def __init__(self, field_names: List[str], field_types: List[DType]) -> None:
        """
        :param field_names: Names of table's columns.
        :param field_types: Data types of table's columns.
        """
        self.field_names = field_names
        self.field_types = field_types

    def __str__(self) -> str:
        values = [
            (i, self.field_names[i], self.field_types[i].to_json())
            for i in range(len(self.field_names))
        ]

        return json.dumps(values, indent=2, sort_keys=True)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Schema)
            and self.field_names == other.field_names
            and self.field_types == other.field_types
        )

    def to_json(self) -> Dict:
        return {
            "field_names": self.field_names,
            "field_types": [field_type.to_json() for field_type in self.field_types],
        }

    def get_field_type(self, field_name: str) -> DType:
        """
        Get the data type of the given field.
        :param field_name: Name of the field.
        :return: THe data type of the given field.
        """
        if field_name not in self.field_names:
            raise FeathubException(f"{field_name} not in the schema.")

        return self.field_types[self.field_names.index(field_name)]

    @staticmethod
    def new_builder() -> "Schema.Builder":
        return Schema.Builder()

    class Builder:
        """
        A builder for constructing a Feathub Schema.
        """

        def __init__(self) -> None:
            self._columns: Dict[str, DType] = {}

        def column(self, column_name: str, dtype: DType) -> "Schema.Builder":
            if column_name in self._columns:
                raise FeathubException(
                    f"Column {column_name} already defined with type {DType}."
                )
            self._columns[column_name] = dtype
            return self

        def build(self) -> "Schema":
            return Schema(list(self._columns.keys()), list(self._columns.values()))

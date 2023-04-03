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
import unittest
from typing import Optional, List, Dict

from feathub.common.exceptions import FeathubException
from feathub.common.types import Int64
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema


class MockFeatureTable(FeatureTable):
    def __init__(
        self,
        name: str,
        schema: Schema,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
    ) -> None:
        super().__init__(
            name=name,
            system_name="mock",
            properties={},
            schema=schema,
            keys=keys,
            timestamp_field=timestamp_field,
        )

    def to_json(self) -> Dict:
        pass


class FeatureTableTest(unittest.TestCase):
    def test_timestamp_field_not_in_schema(self):
        with self.assertRaises(FeathubException) as cm:
            MockFeatureTable(
                name="source",
                schema=Schema(["id", "val1"], [Int64, Int64]),
                timestamp_field="timestamp",
                keys=["id"],
            )
        self.assertIn(
            "Timestamp field 'timestamp' it is not present in the schema",
            cm.exception.args[0],
        )

    def test_key_fields_not_in_schema(self):
        with self.assertRaises(FeathubException) as cm:
            MockFeatureTable(
                name="source",
                schema=Schema(["id2", "val2"], [Int64, Int64]),
                keys=["id1", "id2"],
            )
        self.assertIn(
            "Key field(s) '{'id1'}' is not present in the schema", cm.exception.args[0]
        )

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

from feathub.common import types
from feathub.common.exceptions import FeathubException

from feathub.table.schema import Schema


class SchemaTest(unittest.TestCase):
    def test_schema_builder(self):
        schema = (
            Schema.new_builder()
            .column("a", types.Int64)
            .column("b", types.Int32)
            .column("c", types.Float32)
            .build()
        )

        self.assertEqual(
            Schema(["a", "b", "c"], [types.Int64, types.Int32, types.Float32]), schema
        )

    def test_schema_builder_duplicate_column_name(self):
        with self.assertRaises(FeathubException) as cm:
            Schema.new_builder().column("a", types.Int64).column(
                "a", types.Int32
            ).build()

        self.assertIn("Column a already defined", cm.exception.args[0])

    def test_schema_builder_prohibited_column_name(self):
        with self.assertRaises(FeathubException) as cm:
            Schema.new_builder().column("__event_time_attribute__", types.Int64).build()

        self.assertIn(
            "should not start or end with double underscores(__)", cm.exception.args[0]
        )

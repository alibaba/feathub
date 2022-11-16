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
from typing import cast

from feathub.common.types import Int64
from feathub.feature_tables.sources.datagen_source import (
    DEFAULT_BOUNDED_NUMBER_OF_ROWS,
    DataGenSource,
    SequenceField,
)
from feathub.table.schema import Schema


class DataGenSourceTest(unittest.TestCase):
    def test_boundedness(self):
        source = DataGenSource(
            "source", Schema.new_builder().column("x", Int64).column("y", Int64).build()
        )
        self.assertFalse(source.is_bounded())

        source = DataGenSource(
            "source",
            Schema.new_builder().column("x", Int64).column("y", Int64).build(),
            number_of_rows=10,
        )
        self.assertTrue(source.is_bounded())

        source = DataGenSource(
            "source",
            Schema.new_builder().column("x", Int64).column("y", Int64).build(),
            field_configs={"x": SequenceField(start=1, end=10)},
        )
        self.assertTrue(source.is_bounded())

    def test_get_bounded_feature_table(self):
        source = DataGenSource(
            "source", Schema.new_builder().column("x", Int64).column("y", Int64).build()
        )
        self.assertFalse(source.is_bounded())

        bounded_source = source.get_bounded_view()
        self.assertTrue(bounded_source.is_bounded())
        self.assertEqual(
            DEFAULT_BOUNDED_NUMBER_OF_ROWS,
            cast(DataGenSource, bounded_source).number_of_rows,
        )

        source_json = source.to_json()
        source_json.pop("number_of_rows")
        bounded_source_json = bounded_source.to_json()
        bounded_source_json.pop("number_of_rows")

        self.assertEqual(source_json, bounded_source_json)

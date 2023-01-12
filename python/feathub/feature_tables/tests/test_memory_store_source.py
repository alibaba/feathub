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
import unittest
from typing import cast

from feathub.feature_tables.sources.memory_store_source import MemoryStoreSource


class MemoryStoreSourceTest(unittest.TestCase):
    def test_get_bounded_feature_table(self):
        source = MemoryStoreSource("source", ["id"], "table")
        self.assertTrue(source.is_bounded())

        bounded_source = source.get_bounded_view()
        self.assertTrue(cast(MemoryStoreSource, bounded_source).is_bounded())

        source_json = source.to_json()
        bounded_source_json = bounded_source.to_json()

        self.assertEqual(source_json, bounded_source_json)

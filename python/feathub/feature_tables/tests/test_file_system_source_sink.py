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
from abc import ABC

from feathub.common.types import Int64
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class FileSystemSourceTest(unittest.TestCase):
    def test_get_bounded_feature_table(self):
        source = FileSystemSource(
            "source",
            "./path",
            "csv",
            Schema.new_builder().column("x", Int64).column("y", Int64).build(),
        )
        self.assertTrue(source.is_bounded())


class FileSystemSourceSinkITTest(ABC, FeathubITTestBase):
    pass
    # TODO: unify the structure of files written out by different processors.

    # TODO: Add test case to verify allow_overwrite.

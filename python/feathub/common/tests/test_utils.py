# Copyright 2022 The FeatHub Authors
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

import unittest
from datetime import datetime
from typing import Any, List

import feathub.common.utils as utils
from feathub.common import types
from feathub.table.schema import Schema


class UtilsTest(unittest.TestCase):
    def test_to_java_date_format(self):
        java_format = utils.to_java_date_format("%Y-%m-%d %H:%M:%S")
        self.assertEqual("yyyy-MM-dd HH:mm:ss", java_format)

    def test_to_unix_timestamp(self):
        unix_timestamp = utils.to_unix_timestamp("1970-01-01 00:00:01")
        self.assertEqual(1.0, unix_timestamp)

    def test_to_unix_timestamp_epoch(self):
        self.assertEqual(0.0, utils.to_unix_timestamp(0, format="epoch"))
        self.assertEqual(1.234, utils.to_unix_timestamp(1234, format="epoch_millis"))

    def test_read_write_protobuf(self):
        schema: Schema = (
            Schema.new_builder()
            .column("Bytes", types.Bytes)
            .column("String", types.String)
            .column("Int32", types.Int32)
            .column("Int64", types.Int64)
            .column("Float32", types.Float32)
            .column("Float64", types.Float64)
            .column("Timestamp", types.Timestamp)
            .column("Float64Vector", types.Float64Vector)
            .column("Map_String_Int64", types.MapType(types.String, types.Int64))
            .build()
        )

        original_data: List[List[Any]] = [
            [
                b"Apple",
                "Apple",
                1,
                1,
                1.0,
                1.0,
                datetime.fromtimestamp(1000000),
                [1.0, 1.0],
                {"Apple": 1},
            ],
            [
                b"Banana",
                "Banana",
                2,
                2,
                2.0,
                2.0,
                datetime.fromtimestamp(2000000),
                [2.0, 2.0],
                {"Banana": 2},
            ],
            [
                b"Cherry",
                "Cherry",
                3,
                3,
                3.0,
                3.0,
                datetime.fromtimestamp(3000000),
                [3.0, 3.0],
                {"Cherry": 3},
            ],
            [
                b"Dates",
                "Dates",
                4,
                4,
                4.0,
                4.0,
                datetime.fromtimestamp(4000000),
                [4.0, 4.0],
                {"Dates": 4},
            ],
            [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ]

        serialized_data: List[List[bytes]] = [
            [b"" for _ in row] for row in original_data
        ]
        for i in range(len(original_data)):
            for j in range(len(schema.field_names)):
                serialized_data[i][j] = utils.serialize_object_with_protobuf(
                    original_data[i][j], schema.field_types[j]
                )

        deserialized_data = [[None for _ in row] for row in original_data]
        for i in range(len(original_data)):
            for j in range(len(schema.field_names)):
                deserialized_data[i][j] = utils.deserialize_object_with_protobuf(
                    serialized_data[i][j]
                )

        self.assertEqual(len(original_data), len(deserialized_data))
        for i in range(len(original_data)):
            self.assertListEqual(original_data[i], deserialized_data[i])

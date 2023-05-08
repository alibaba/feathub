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
from typing import Dict

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    TableDescriptor,
    DataTypes,
    StreamTableEnvironment,
)

from feathub.common.exceptions import FeathubTypeException
from feathub.common.types import (
    Bytes,
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Int32Vector,
    Unknown,
    DType,
    MapType,
)
from feathub.processors.flink.flink_types_utils import (
    to_flink_schema,
    to_feathub_schema,
    to_feathub_type,
    to_flink_type,
    to_flink_sql_type,
)
from feathub.table.schema import Schema


class FlinkTypeUtilsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.t_env = StreamTableEnvironment.create(self.env)
        field_names = [
            "bytes",
            "string",
            "int32",
            "int64",
            "float32",
            "float64",
            "bool",
            "int32vector",
            "map",
        ]
        field_types = [
            Bytes,
            String,
            Int32,
            Int64,
            Float32,
            Float64,
            Bool,
            Int32Vector,
            MapType(String, Int64),
        ]
        self.schema = Schema(field_names, field_types)

    def test_schema_conversion(self):
        flink_schema = to_flink_schema(self.schema)
        table = self.t_env.from_descriptor(
            TableDescriptor.for_connector("datagen").schema(flink_schema).build()
        )

        self.assertEqual(self.schema, to_feathub_schema(table.get_schema()))

    def test_sql_type(self):
        field_name_type = []
        for field_name, field_type in zip(
            self.schema.field_names, self.schema.field_types
        ):
            field_name_type.append(f"`{field_name}` {to_flink_sql_type(field_type)}")
        schema_sql = ", ".join(field_name_type)
        self.t_env.execute_sql(
            f"CREATE TABLE test_table ({schema_sql}) WITH ('connector' = 'datagen');"
        ).wait()
        table = self.t_env.from_path("test_table")
        self.assertEqual(self.schema, to_feathub_schema(table.get_schema()))

    def test_unsupported_flink_type_throw_exception(self):
        tiny_int_type = DataTypes.TINYINT()
        with self.assertRaises(FeathubTypeException):
            to_feathub_type(tiny_int_type)

        map_type = DataTypes.ROW()
        with self.assertRaises(FeathubTypeException):
            to_feathub_type(map_type)

        nested_array = DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))
        with self.assertRaises(FeathubTypeException):
            to_feathub_type(nested_array)

    def test_unsupported_feathub_type_throw_exception(self):
        with self.assertRaises(FeathubTypeException):
            to_flink_type(Unknown)

        class InvalidType(DType):
            def to_json(self) -> Dict:
                return {}

        with self.assertRaises(FeathubTypeException):
            to_flink_type(InvalidType())

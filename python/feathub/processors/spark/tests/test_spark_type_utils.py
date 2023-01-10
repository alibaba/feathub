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
from typing import Dict

from pyspark.sql import types as native_spark_types

from feathub.common.exceptions import FeathubTypeException
from feathub.common import types
from feathub.processors.spark.spark_types_utils import (
    to_spark_struct_type,
    to_feathub_schema,
    to_feathub_type,
    to_spark_type,
)
from feathub.table.schema import Schema


class SparkTypeUtilsTest(unittest.TestCase):
    def test_schema_conversion(self):
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
            types.Bytes,
            types.String,
            types.Int32,
            types.Int64,
            types.Float32,
            types.Float64,
            types.Bool,
            types.Int32Vector,
            types.MapType(types.String, types.Int64),
        ]
        schema = Schema(field_names, field_types)

        self.assertEqual(schema, to_feathub_schema(to_spark_struct_type(schema)))

    def test_unsupported_spark_type_throw_exception(self):
        byte_type = native_spark_types.ByteType()
        with self.assertRaises(FeathubTypeException):
            to_feathub_type(byte_type)

        nested_array = native_spark_types.ArrayType(
            native_spark_types.ArrayType(native_spark_types.IntegerType())
        )
        with self.assertRaises(FeathubTypeException):
            to_feathub_type(nested_array)

    def test_unsupported_feathub_type_throw_exception(self):
        with self.assertRaises(FeathubTypeException):
            to_spark_type(types.Unknown)

        class InvalidType(types.DType):
            def to_json(self) -> Dict:
                return {}

        with self.assertRaises(FeathubTypeException):
            to_spark_type(InvalidType())

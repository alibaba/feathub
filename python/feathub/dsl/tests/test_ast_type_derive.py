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

from feathub.common.exceptions import FeathubExpressionException
from feathub.common.types import (
    Int32,
    Bool,
    Float64,
    String,
    Int64,
    Float32,
    Bytes,
    Timestamp,
    DType,
)
from feathub.dsl.expr_parser import ExprParser


class AstTypeDeriverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.expr_parser = ExprParser()

    def test_binary_op_type_derive(self):
        expected_types = [
            # (LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE)
            (Int32, Int32, Int32),
            (Int32, Int64, Int64),
            (Int32, Float32, Float32),
            (Int32, Float64, Float64),
            (Int64, Int64, Int64),
            (Int64, Float32, Float32),
            (Int64, Float64, Float64),
            (Float32, Float32, Float32),
            (Float32, Float64, Float64),
            (Float64, Float64, Float64),
        ]

        for left_type, right_type, result_type in expected_types:
            ast = self.expr_parser.parse("a + b")
            actual_result_type = ast.eval_dtype({"a": left_type, "b": right_type})
            self.assertEqual(result_type, actual_result_type)

        with self.assertRaises(FeathubExpressionException):
            ast = self.expr_parser.parse("a + b")
            ast.eval_dtype({"a": Int32, "b": String})

    def test_variable_node_type_derive(self):
        ast = self.expr_parser.parse("a")
        self.assertEqual(Int32, ast.eval_dtype({"a": Int32}))

    def test_uminus_node_type_derive(self):
        ast = self.expr_parser.parse("-a")
        self.assertEqual(Int32, ast.eval_dtype({"a": Int32}))

    def test_compare_node_type_derive(self):
        ast = self.expr_parser.parse("a < 1")
        self.assertEqual(Bool, ast.eval_dtype({"a": Int32}))

    def test_value_node_type_derive(self):
        ast = self.expr_parser.parse("1")
        self.assertEqual(Int32, ast.eval_dtype({}))

        ast = self.expr_parser.parse("1.0")
        self.assertEqual(Float64, ast.eval_dtype({}))

        ast = self.expr_parser.parse("true")
        self.assertEqual(Bool, ast.eval_dtype({}))

        ast = self.expr_parser.parse("'FeatHub'")
        self.assertEqual(String, ast.eval_dtype({}))

    def test_type_cast_type_derive(self):
        ast = self.expr_parser.parse("CAST(1 AS INTEGER)")
        self.assertEqual(Int32, ast.eval_dtype({}))

        ast = self.expr_parser.parse("CAST(1 AS BIGINT)")
        self.assertEqual(Int64, ast.eval_dtype({}))

        ast = self.expr_parser.parse("CAST(1 AS FLOAT)")
        self.assertEqual(Float32, ast.eval_dtype({}))

        ast = self.expr_parser.parse("CAST(1 AS DOUBLE)")
        self.assertEqual(Float64, ast.eval_dtype({}))

        ast = self.expr_parser.parse("CAST(1 AS STRING)")
        self.assertEqual(String, ast.eval_dtype({}))

        ast = self.expr_parser.parse("CAST(1 AS BYTES)")
        self.assertEqual(Bytes, ast.eval_dtype({}))

        ast = self.expr_parser.parse("CAST(1 AS BOOLEAN)")
        self.assertEqual(Bool, ast.eval_dtype({}))

        ast = self.expr_parser.parse("CAST(1 AS TIMESTAMP)")
        self.assertEqual(Timestamp, ast.eval_dtype({}))

    def test_logical_op_type_derive(self):
        ast = self.expr_parser.parse("true or False")
        self.assertEqual(Bool, ast.eval_dtype({}))

    def test_is_op_result_type(self):
        ast = self.expr_parser.parse("a is NULL")
        self.assertEqual(Bool, ast.eval_dtype({}))

    def test_function_call_result_type(self):
        ast = self.expr_parser.parse("LOWER('ABC')")
        self.assertEqual(String, ast.eval_dtype({}))

        ast = self.expr_parser.parse("UNIX_TIMESTAMP('2022-01-01 00:00:00')")
        self.assertEqual(Int64, ast.eval_dtype({}))

    def test_group_node_result_type(self):
        ast = self.expr_parser.parse("a * (b + c)")
        self.assertEqual(
            Int64,
            ast.eval_dtype({"a": Int32, "b": Int64, "c": Int64}),
        )

    def test_case_op_result_type(self):
        ast = self.expr_parser.parse("CASE WHEN TRUE THEN a ELSE b END")
        self.assertEqual(
            Int32,
            ast.eval_dtype({"a": Int32, "b": Int32}),
        )
        self.assertEqual(
            Float32,
            ast.eval_dtype({"a": Int32, "b": Float32}),
        )

        with self.assertRaises(FeathubExpressionException):
            ast.eval_dtype({"a": Int32, "b": String})

        ast = self.expr_parser.parse(
            "CASE WHEN TRUE THEN a WHEN TRUE THEN b WHEN TRUE THEN c END"
        )
        self.assertEqual(
            Float32, ast.eval_dtype({"a": Int32, "b": Int64, "c": Float32})
        )

    def _to_sql_type(self, dtype: DType) -> str:
        if dtype == Int32:
            return "INTEGER"
        elif dtype == Int64:
            return "BIGINT"
        elif dtype == Float32:
            return "FLOAT"
        elif dtype == Float64:
            return "DOUBLE"
        else:
            raise RuntimeError("Unknown type.")

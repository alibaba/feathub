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
from datetime import datetime

from feathub.common.exceptions import FeathubException, FeathubExpressionException
from feathub.dsl.expr_parser import ExprParser
from feathub.processors.local.ast_evaluator.local_ast_evaluator import LocalAstEvaluator


class LocalAstEvaluatorTest(unittest.TestCase):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.parser = ExprParser()
        self.ast_evaluator = LocalAstEvaluator()

    def _eval(self, expr, variables=None):
        ast = self.parser.parse(expr)
        return self.ast_evaluator.eval(ast, variables)

    def test_binary_op(self):
        expr = "1 + 2 * 3 - 6"
        self.assertEqual(1, self._eval(expr))
        expr = "(1 + 2) * 3 - 6"
        self.assertEqual(3, self._eval(expr))
        expr = "2 * 3 / 5"
        self.assertEqual(1.2, self._eval(expr))

    def test_uminus(self):
        expr = "-6 + 9"
        self.assertEqual(3, self._eval(expr))
        expr = "-a + 9"
        self.assertEqual(3, self._eval(expr, {"a": 6}))

    def test_compare_op(self):
        expr = "(1 + 2) > 3"
        self.assertEqual(False, self._eval(expr))
        expr = "(1 + 2) >= 3"
        self.assertEqual(True, self._eval(expr))
        expr = "(1 + 2) < 3"
        self.assertEqual(False, self._eval(expr))
        expr = "(1 + 2) <= 3"
        self.assertEqual(True, self._eval(expr))
        expr = "(1 + 2) = 3"
        self.assertEqual(True, self._eval(expr))
        expr = "(1 + 2) <> 3"
        self.assertEqual(False, self._eval(expr))
        expr = "(a + b) <> 3"
        self.assertEqual(False, self._eval(expr, {"a": 1, "b": 2}))

    def test_variable(self):
        expr = "1 + 2 * x"
        self.assertEqual(6, self._eval(expr, {"x": 2.5}))
        expr = "x * x - y"
        self.assertEqual(3, self._eval(expr, {"x": 3, "y": 6}))

    def test_func_call(self):
        expr = """
        unix_timestamp("2020-01-01 00:24:39") - unix_timestamp("2020-01-01 00:23:40")
        """
        self.assertEqual(59, self._eval(expr))

        expr = """
        unix_timestamp("2020-01-01 00:24:39") - unix_timestamp(ts)
        """
        self.assertEqual(59, self._eval(expr, {"ts": "2020-01-01 00:23:40"}))

    def test_cast(self):
        self.assertEqual(b"59", self._eval('CAST("59" AS bytes)'))
        self.assertEqual("0.1", self._eval("CAST(0.1 AS STRING)"))
        self.assertEqual(59, self._eval('CAST("59" AS INTEGER)'))
        self.assertEqual(59, self._eval("CAST(a AS INTEGER)", {"a": "59"}))
        self.assertEqual(59, self._eval('CAST("59" AS BIGINT)'))
        self.assertEqual(59.0, self._eval('CAST("59" AS FLOAT)'))
        self.assertEqual(59.0, self._eval('CAST("59" AS DOUBLE)'))
        self.assertEqual(True, self._eval('CAST("true" AS BOOLEAN)'))
        self.assertEqual(True, self._eval("CAST(1 AS BOOLEAN)"))
        self.assertEqual(False, self._eval('CAST("false" AS BOOLEAN)'))
        with self.assertRaises(FeathubException) as cm:
            self.assertEqual(False, self._eval('CAST("invalid" AS BOOLEAN)'))
        self.assertIn("Cannot parser", cm.exception.args[0])
        self.assertEqual(
            datetime(2022, 1, 1, 0, 0, 0, 1000),
            self._eval('CAST("2022-01-01 00:00:00.001" AS TIMESTAMP)'),
        )

        self.assertEqual("59", self._eval("TRY_CAST(59 AS STRING)"))
        self.assertEqual(None, self._eval('TRY_CAST("INVALID" AS DOUBLE)'))

    def test_logical_op(self):
        self.assertEqual(True, self._eval("false OR True"))
        self.assertEqual(True, self._eval("a OR True", {"a": True}))
        self.assertEqual(False, self._eval("true AND FALSE"))

    def test_null_node(self):
        self.assertEqual(None, self._eval("NULL"))

    def test_is_op(self):
        self.assertEqual(True, self._eval("a IS NULL", {"a": None}))
        self.assertEqual(False, self._eval("a IS NULL", {"a": 1}))
        self.assertEqual(False, self._eval("a IS NOT NULL", {"a": None}))
        self.assertEqual(True, self._eval("a IS NOT NULL", {"a": 1}))

    def test_case_op(self):
        expr = "CASE WHEN a > b THEN 1 WHEN a < b THEN 2 ELSE 3 END"
        self.assertEqual(1, self._eval(expr, {"a": 2, "b": 1}))
        expr = "CASE WHEN a > b THEN 1 WHEN a < b THEN 2 ELSE 3 END"
        self.assertEqual(2, self._eval(expr, {"a": 1, "b": 2}))
        expr = "CASE WHEN a > b THEN 1 WHEN a < b THEN 2 ELSE 3 END"
        self.assertEqual(3, self._eval(expr, {"a": 1, "b": 1}))

        with self.assertRaises(FeathubExpressionException):
            self._eval("CASE WHEN 1 + 1 THEN 2 END")

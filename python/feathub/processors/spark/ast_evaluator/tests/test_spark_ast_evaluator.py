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

from feathub.dsl.expr_parser import ExprParser
from feathub.processors.spark.ast_evaluator.spark_ast_evaluator import SparkAstEvaluator


class SparkAstEvaluatorTest(unittest.TestCase):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.parser = ExprParser()
        self.ast_evaluator = SparkAstEvaluator()

    def _eval(self, expr, variables=None):
        ast = self.parser.parse(expr)
        return self.ast_evaluator.eval(ast, variables)

    def test_binary_op(self):
        expr = "1 +2 * 3 - 6"
        self.assertEqual("1 + 2 * 3 - 6", self._eval(expr))
        expr = "(1 +2) *3 - 6"
        self.assertEqual("(1 + 2) * 3 - 6", self._eval(expr))
        expr = "2 * 3 /5"
        self.assertEqual("2 * 3 / 5", self._eval(expr))

    def test_uminus(self):
        expr = "-6 + 9"
        self.assertEqual("-6 + 9", self._eval(expr))
        expr = "-a + 9"
        self.assertEqual("-`a` + 9", self._eval(expr))

    def test_compare_op(self):
        expr = "(1 + 2) > 3"
        self.assertEqual("(1 + 2) > 3", self._eval(expr))
        expr = "(1 + 2) >= 3"
        self.assertEqual("(1 + 2) >= 3", self._eval(expr))
        expr = "(1 + 2) < 3"
        self.assertEqual("(1 + 2) < 3", self._eval(expr))
        expr = "(1 + 2) <= 3"
        self.assertEqual("(1 + 2) <= 3", self._eval(expr))
        expr = "(1 + 2) = 3"
        self.assertEqual("(1 + 2) = 3", self._eval(expr))
        expr = "(1 + 2) <> 3"
        self.assertEqual("(1 + 2) <> 3", self._eval(expr))
        expr = "(a + b) <> 3"
        self.assertEqual("(`a` + `b`) <> 3", self._eval(expr))

    def test_variable(self):
        expr = "1 + 2 * x"
        self.assertEqual("1 + 2 * `x`", self._eval(expr))
        expr = "x * x - y"
        self.assertEqual("`x` * `x` - `y`", self._eval(expr))

    def test_func_call(self):
        expr = """
        UNIX_TIMESTAMP("2020-01-01 00:24:39") - UNIX_TIMESTAMP("2020-01-01 00:23:40")
        """
        self.assertEqual(
            "TO_UNIX_TIMESTAMP('2020-01-01 00:24:39') "
            "- TO_UNIX_TIMESTAMP('2020-01-01 00:23:40')",
            self._eval(expr),
        )

        expr = """
        UNIX_TIMESTAMP("2020-01-01 00:24:39") - UNIX_TIMESTAMP(ts)
        """
        self.assertEqual(
            "TO_UNIX_TIMESTAMP('2020-01-01 00:24:39') - TO_UNIX_TIMESTAMP(`ts`)",
            self._eval(expr, {"ts": "2020-01-01 00:23:40"}),
        )

    def test_cast(self):
        self.assertEqual("CAST('59' AS BYTES)", self._eval('cast("59" AS bytes)'))
        self.assertEqual("CAST(0.1 AS STRING)", self._eval("CAST(0.1 AS STRING)"))
        self.assertEqual("CAST('59' AS INTEGER)", self._eval('CAST("59" AS INTEGER)'))
        self.assertEqual("CAST(`a` AS INTEGER)", self._eval("CAST(a AS INTEGER)"))
        self.assertEqual("CAST('59' AS BIGINT)", self._eval('CAST("59" AS BIGINT)'))
        self.assertEqual("CAST('59' AS FLOAT)", self._eval('CAST("59" AS FLOAT)'))
        self.assertEqual("CAST('59' AS DOUBLE)", self._eval('CAST("59" AS DOUBLE)'))
        self.assertEqual(
            "CAST('true' AS BOOLEAN)", self._eval('CAST("true" AS BOOLEAN)')
        )
        self.assertEqual("CAST(1 AS BOOLEAN)", self._eval("CAST(1 AS BOOLEAN)"))
        self.assertEqual(
            "CAST('false' AS BOOLEAN)", self._eval('CAST("false" AS BOOLEAN)')
        )
        self.assertEqual(
            "CAST('invalid' AS BOOLEAN)", self._eval('CAST("invalid" AS BOOLEAN)')
        )
        self.assertEqual(
            "CAST('2022-01-01 00:00:00.001' AS TIMESTAMP)",
            self._eval('CAST("2022-01-01 00:00:00.001" AS TIMESTAMP)'),
        )

        self.assertEqual("TRY_CAST(59 AS STRING)", self._eval("TRY_CAST(59 AS STRING)"))

    def test_logical_op(self):
        self.assertEqual("false || true", self._eval("false || True"))
        self.assertEqual("`a` || true", self._eval("a || True"))
        self.assertEqual("true && false", self._eval("true && FALSE"))

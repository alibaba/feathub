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
from typing import List, Any

from ply import lex

from feathub.common.exceptions import FeathubExpressionException
from feathub.dsl.expr_lexer_rules import ExprLexerRules


class TestExprLexerRules(unittest.TestCase):
    def setUp(self) -> None:
        self.lexer = lex.lex(module=ExprLexerRules())

    # Analyze the given data
    def tokenize(self, data: str) -> List[lex.LexToken]:
        self.lexer.input(data)
        tokens = []
        while True:
            tok = self.lexer.token()
            if not tok:
                break
            tokens.append(tok)
        return tokens

    def check_token_types_values(
        self,
        expected_token_types: List[str],
        expected_token_values: List[Any],
        actual_tokens: List[lex.LexToken],
    ):
        self.assertEqual(expected_token_types, [t.type for t in actual_tokens])
        self.assertEqual(expected_token_values, [t.value for t in actual_tokens])

    def test_binop_lex(self):
        self.check_token_types_values(
            ["ID", "+", "ID"], ["a", "+", "b"], self.tokenize("a + b")
        )

        self.check_token_types_values(
            ["INTEGER", "+", "FLOAT"], [1, "+", 1.2], self.tokenize("1 + 1.2")
        )

    def test_uminus(self):
        self.check_token_types_values(["-", "ID"], ["-", "a"], self.tokenize("-a"))
        self.check_token_types_values(["-", "FLOAT"], ["-", 0.1], self.tokenize("-0.1"))

    def test_lexer_complex(self):
        expr = """
        cast ("0.1" AS FLOAT) + CAST("1" AS INTEGER) || "abc" = "abc" && `integer` <> 1
        """
        token_types = (
            "CAST LPAREN STRING AS DTYPE RPAREN + CAST LPAREN STRING "
            "AS DTYPE RPAREN OR STRING EQ STRING AND ID NE INTEGER"
        ).split(" ")
        token_values: List[Any] = (
            'CAST ( "0.1" AS FLOAT ) + CAST ( "1" AS INTEGER ) || '
            '"abc" = "abc" && integer <>'
        ).split(" ")
        token_values.append(1)
        self.check_token_types_values(
            token_types,
            token_values,
            self.tokenize(expr),
        )

    def test_reserved_word_as_id(self):
        self.check_token_types_values(
            ["CAST", "LPAREN", "ID", "AS", "DTYPE", "RPAREN"],
            ["CAST", "(", "integer", "AS", "INTEGER", ")"],
            self.tokenize("CAST (`integer` AS integer)"),
        )

    def test_lex_error(self):
        with self.assertRaises(FeathubExpressionException) as cm:
            self.tokenize("!@#")

        self.assertIn("Illegal character", cm.exception.args[0])

    def test_cast(self):
        self.check_token_types_values(
            ["CAST", "LPAREN", "ID", "AS", "DTYPE", "RPAREN"],
            ["CAST", "(", "a", "AS", "DOUBLE", ")"],
            self.tokenize("CAST(a AS DOUBLE)"),
        )

        self.check_token_types_values(
            ["TRY_CAST", "LPAREN", "ID", "AS", "DTYPE", "RPAREN"],
            ["TRY_CAST", "(", "a", "AS", "DOUBLE", ")"],
            self.tokenize("TRY_CAST(a AS DOUBLE)"),
        )

    def test_logical_op(self):
        self.check_token_types_values(
            ["ID", "OR", "ID"], ["a", "||", "b"], self.tokenize("a || b")
        )
        self.check_token_types_values(
            ["ID", "AND", "ID"], ["a", "&&", "b"], self.tokenize("a && b")
        )
        self.check_token_types_values(
            ["FALSE", "OR", "TRUE"],
            [False, "||", True],
            self.tokenize("false || true"),
        )
        self.check_token_types_values(
            ["TRUE", "AND", "FALSE"],
            [True, "&&", False],
            self.tokenize("true && false"),
        )

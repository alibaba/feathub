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
from typing import Dict, Tuple, Optional, Any

from ply import lex
from ply.lex import TOKEN

from feathub.common.exceptions import FeathubExpressionException


class ExprLexerRules:
    literals = ["+", "-", "*", "/"]

    # Map from reserved keywords to its token type and token value. If token value is
    # None, the value is set to the text that matches the reserved keywords.
    reserved: Dict[str, Tuple[str, Optional[Any]]] = {
        # 'if': 'IF',
        # 'then': 'THEN',
        # 'else': 'ELSE',
        # 'while': 'WHILE',
    }

    # List of token names. This is always required
    tokens = [
        "LPAREN",
        "RPAREN",
        "LT",
        "LE",
        "GT",
        "GE",
        "EQ",
        "NE",
        "COMMA",
        "FLOAT",
        "INTEGER",
        "STRING",
        "ID",
    ] + list(set([v[0] for v in reserved.values()]))

    # Regular expression rules for simple tokens
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_LT = r"<"
    t_LE = r"<="
    t_GT = r">"
    t_GE = r">="
    t_EQ = r"==?"
    t_NE = r"<>"
    t_COMMA = r"\,"
    t_STRING = r"(\".*?\"|\'.*?\')"

    # A string containing ignored characters (spaces and tabs)
    t_ignore = " \t"

    @TOKEN(r"((\d*\.\d+)(E[\+-]?\d+)?|([1-9]\d*E[\+-]?\d+))")
    def t_FLOAT(self, t: lex.LexToken) -> lex.LexToken:
        t.value = float(t.value)
        return t

    @TOKEN(r"\d+")
    def t_INTEGER(self, t: lex.LexToken) -> lex.LexToken:
        t.value = int(t.value)
        return t

    @TOKEN(r"[a-zA-Z_][a-zA-Z0-9_]*")
    def t_ID(self, t: lex.LexToken) -> lex.LexToken:
        t.type = ExprLexerRules.reserved.get(
            t.value.lower(), "ID"
        )  # Check for reserved words
        return t

    # Define a rule so we can track line numbers
    @TOKEN(r"\n+")
    def t_newline(self, t: lex.LexToken) -> None:
        t.lexer.lineno += len(t.value)

    # Error handling rule
    def t_error(self, t: lex.LexToken) -> None:
        raise FeathubExpressionException(f"Illegal character '{t.value[0]}'")

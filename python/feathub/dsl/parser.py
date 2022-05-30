# Copyright 2022 The Feathub Authors
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

import logging
import ply.lex as lex
import ply.yacc as yacc
from ply.lex import TOKEN

from feathub.dsl.ast import BinaryOp
from feathub.dsl.ast import UminusOp
from feathub.dsl.ast import CompareOp
from feathub.dsl.ast import ValueNode
from feathub.dsl.ast import FuncCallOp
from feathub.dsl.ast import ArgListNode
from feathub.dsl.ast import VariableNode
from feathub.dsl.ast import ExprAST


class ExprParser:

    literals = ["+", "-", "*", "/"]

    reserved = {
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
    ] + list(reserved.values())

    # Regular expression rules for simple tokens
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_LT = r"<"
    t_LE = r"<="
    t_GT = r">"
    t_GE = r">="
    t_EQ = r"=="
    t_NE = r"<>"
    t_COMMA = r"\,"
    t_STRING = r"(\".*?\"|\'.*?\')"

    # A string containing ignored characters (spaces and tabs)
    t_ignore = " \t"

    precedence = (
        ("left", "LT", "LE", "GT", "GE", "EQ", "NE"),
        ("left", "+", "-"),
        ("left", "*", "/"),
        ("right", "UMINUS"),
    )

    def __init__(self, **kwargs):
        logging.basicConfig(
            level=logging.DEBUG,
            filename=".parser.out",
            filemode="w",
            format="%(filename)10s:%(lineno)4d:%(message)s",
        )
        log = logging.getLogger()
        self.lexer = lex.lex(module=self, **kwargs)
        self.yacc = yacc.yacc(module=self, write_tables=False, debuglog=log)

    @TOKEN(r"((\d*\.\d+)(E[\+-]?\d+)?|([1-9]\d*E[\+-]?\d+))")
    def t_FLOAT(self, t):
        t.value = float(t.value)
        return t

    @TOKEN(r"\d+")
    def t_INTEGER(self, t):
        t.value = int(t.value)
        return t

    @TOKEN(r"[a-zA-Z_][a-zA-Z0-9_]*")
    def t_ID(self, t):
        t.type = ExprParser.reserved.get(t.value, "ID")  # Check for reserved words
        return t

    # Define a rule so we can track line numbers
    @TOKEN(r"\n+")
    def t_newline(self, t):
        t.lexer.lineno += len(t.value)

    # Error handling rule
    def t_error(self, t):
        print(f"Illegal character '{t.value[0]}'")
        t.lexer.skip(1)

    def p_expression_binop(self, p):
        """
        expression : expression '+' expression
                   | expression '-' expression
                   | expression '*' expression
                   | expression '/' expression
        """
        p[0] = BinaryOp(op_type=p[2], left_child=p[1], right_child=p[3])

    def p_expression_uminus(self, p):
        "expression : '-' expression %prec UMINUS"
        p[0] = UminusOp(p[2])

    def p_expression_compare(self, p):
        """
        expression : expression LT expression
                   | expression LE expression
                   | expression GT expression
                   | expression GE expression
                   | expression EQ expression
                   | expression NE expression
        """
        p[0] = CompareOp(op_type=p[2], left_child=p[1], right_child=p[3])

    def p_expression_group(self, p):
        "expression : LPAREN expression RPAREN"
        p[0] = p[2]

    def p_expression_number(self, p):
        """
        expression : FLOAT
                   | INTEGER
        """
        p[0] = ValueNode(p[1])

    def p_expression_string(self, p):
        """
        expression : STRING
        """
        p[0] = ValueNode(p[1][1:-1])

    def p_expression_function_call(self, p):
        "expression : ID LPAREN arglist RPAREN"
        p[0] = FuncCallOp(p[1], p[3])

    def p_expression_arglist(self, p):
        """
        arglist : arglist COMMA expression
                | expression
        """
        if len(p) == 4:
            p[0] = p[1]
            p[0].values.append(p[3])
        else:
            p[0] = ArgListNode([p[1]])

    def p_expression_variable(self, p):
        "expression : ID"
        try:
            p[0] = VariableNode(p[1])
        except LookupError:
            raise RuntimeError(f"Undefined variable name '{p[1]}'.")

    def p_error(self, p):
        if p:
            print(f"Syntax error at '{p.value}'")
        else:
            print("Syntax error at EOF")

    # Analyze the given data
    def tokenize(self, data):
        self.lexer.input(data)
        while True:
            tok = self.lexer.token()
            if not tok:
                break
            print(tok)

    def parse(self, expr) -> ExprAST:
        return self.yacc.parse(expr)

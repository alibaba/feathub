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

import logging
from typing import Any

from ply import lex, yacc

from feathub.common.exceptions import FeathubExpressionException
from feathub.dsl.ast import (
    ExprAST,
    BinaryOp,
    UminusOp,
    CompareOp,
    ValueNode,
    FuncCallOp,
    ArgListNode,
    VariableNode,
    GroupNode,
)
from feathub.dsl.expr_lexer_rules import ExprLexerRules


class ExprParser:

    precedence = (
        ("left", "LT", "LE", "GT", "GE", "EQ", "NE"),
        ("left", "+", "-"),
        ("left", "*", "/"),
        ("right", "UMINUS"),
    )

    def __init__(self, **kwargs: Any) -> None:
        logging.basicConfig(
            level=logging.DEBUG,
            filename=".parser.out",
            filemode="w",
            format="%(filename)10s:%(lineno)4d:%(message)s",
        )
        log = logging.getLogger()
        feathub_expr_lexer_rules = ExprLexerRules()
        self.lexer = lex.lex(module=feathub_expr_lexer_rules, **kwargs)
        self.tokens = feathub_expr_lexer_rules.tokens
        self.yacc = yacc.yacc(module=self, write_tables=False, debuglog=log)

    def p_expression_binop(self, p: yacc.YaccProduction) -> None:
        """
        expression : expression '+' expression
                   | expression '-' expression
                   | expression '*' expression
                   | expression '/' expression
        """
        p[0] = BinaryOp(p[2], p[1], p[3])

    def p_expression_uminus(self, p: yacc.YaccProduction) -> None:
        """expression : '-' expression %prec UMINUS"""
        p[0] = UminusOp(p[2])

    def p_expression_compare(self, p: yacc.YaccProduction) -> None:
        """
        expression : expression LT expression
                   | expression LE expression
                   | expression GT expression
                   | expression GE expression
                   | expression EQ expression
                   | expression NE expression
        """
        p[0] = CompareOp(p[2], p[1], p[3])

    def p_expression_group(self, p: yacc.YaccProduction) -> None:
        """expression : LPAREN expression RPAREN"""
        p[0] = GroupNode(p[2])

    def p_expression_number(self, p: yacc.YaccProduction) -> None:
        """
        expression : FLOAT
                   | INTEGER
        """
        p[0] = ValueNode(p[1])

    def p_expression_string(self, p: yacc.YaccProduction) -> None:
        """
        expression : STRING
        """
        p[0] = ValueNode(p[1][1:-1])

    def p_expression_function_call(self, p: yacc.YaccProduction) -> None:
        """expression : ID LPAREN arglist RPAREN"""
        p[0] = FuncCallOp(p[1], p[3])

    def p_expression_arglist(self, p: yacc.YaccProduction) -> None:
        """
        arglist : arglist COMMA expression
                | expression
        """
        if len(p) == 4:
            p[0] = p[1].values.append(p[3])
        else:
            p[0] = ArgListNode([p[1]])

    def p_expression_variable(self, p: yacc.YaccProduction) -> None:
        """expression : ID"""
        p[0] = VariableNode(p[1])

    def p_error(self, p: yacc.YaccProduction) -> None:  # noqa
        if p:
            raise FeathubExpressionException(f"Syntax error at '{p.value}'")
        else:
            raise FeathubExpressionException("Syntax error at EOF")

    def parse(self, expr: str) -> ExprAST:
        return self.yacc.parse(expr)

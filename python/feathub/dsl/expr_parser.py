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
    CastOp,
    LogicalOp,
    GroupNode,
    CaseOp,
    IsOp,
    NullNode,
    BracketOp,
)
from feathub.dsl.expr_lexer_rules import ExprLexerRules


class ExprParser:
    """
    Expr Parser parses the FeatHub expression and builds the Abstract Syntax Tree(AST).
    The AST will be further evaluated by the AST evaluator of each Processor.
    """

    precedence = (
        ("left", "OR"),
        ("left", "AND"),
        ("left", "LT", "LE", "GT", "GE", "EQ", "NE", "IS", "NOT"),
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

    def p_expression_boolean(self, p: yacc.YaccProduction) -> None:
        """
        expression : TRUE
                   | FALSE
        """
        p[0] = ValueNode(p[1])

    def p_expression_function_call(self, p: yacc.YaccProduction) -> None:
        """
        expression : ID LPAREN arglist RPAREN
                   | ID LPAREN RPAREN
        """
        if len(p) == 5:
            p[0] = FuncCallOp(p[1], p[3])
        else:
            p[0] = FuncCallOp(p[1], ArgListNode([]))

    def p_expression_arglist(self, p: yacc.YaccProduction) -> None:
        """
        arglist : arglist COMMA expression
                | expression
        """
        if len(p) == 4:
            p[0] = ArgListNode(p[1].values.copy())
            p[0].values.append(p[3])
        else:
            p[0] = ArgListNode([p[1]])

    def p_expression_variable(self, p: yacc.YaccProduction) -> None:
        """expression : ID"""
        p[0] = VariableNode(p[1])

    def p_expression_cast(self, p: yacc.YaccProduction) -> None:
        """
        expression : CAST LPAREN expression AS DTYPE RPAREN
                   | TRY_CAST LPAREN expression AS DTYPE RPAREN
        """
        if p[1] == "CAST":
            p[0] = CastOp(p[3], p[5])
            return

        if p[1] == "TRY_CAST":
            p[0] = CastOp(p[3], p[5], exception_on_failure=False)
            return

        raise FeathubExpressionException(f"Unknown cast type {p[1]}.")

    def p_expression_logical_op(self, p: yacc.YaccProduction) -> None:
        """
        expression : expression OR expression
                   | expression AND expression
        """
        p[0] = LogicalOp(p[2], p[1], p[3])

    def p_expression_null_node(self, p: yacc.YaccProduction) -> None:
        """expression : NULL"""
        p[0] = NullNode()

    def p_expression_is_op(self, p: yacc.YaccProduction) -> None:
        """
        expression : expression IS expression
                   | expression IS NOT expression
        """
        if len(p) == 4:
            p[0] = IsOp(p[1], p[3])
        else:
            p[0] = IsOp(p[1], p[4], True)

    def p_expression_case_op_enclose(self, p: yacc.YaccProduction) -> None:
        """
        expression : CASE caselist END
                   | CASE caselist ELSE expression END
        """
        if len(p) == 4:
            p[0] = p[2].build()
        else:
            p[0] = p[2].default(p[4]).build()

    def p_expression_case_op_condition(self, p: yacc.YaccProduction) -> None:
        """
        caselist : caselist WHEN expression THEN expression
                 | WHEN expression THEN expression
        """
        if len(p) == 5:
            p[0] = CaseOp.new_builder().case(p[2], p[4])
        else:
            p[0] = p[1].case(p[3], p[5])

    def p_expression_bracket_op(self, p: yacc.YaccProduction) -> None:
        """
        expression : expression LBRACKET expression RBRACKET
        """
        p[0] = BracketOp(p[1], p[3])

    def p_error(self, p: yacc.YaccProduction) -> None:  # noqa
        if p:
            raise FeathubExpressionException(f"Syntax error at '{p.value}'")
        else:
            raise FeathubExpressionException("Syntax error at EOF")

    def parse(self, expr: str) -> ExprAST:
        return self.yacc.parse(expr)

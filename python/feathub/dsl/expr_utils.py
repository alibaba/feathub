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

import re
from typing import Set, cast, Any, Tuple

from ply import lex

from feathub.dsl.ast import BracketOp, VariableNode, ValueNode
from feathub.dsl.built_in_func import BUILTIN_FUNC_DEF_MAP
from feathub.dsl.expr_lexer_rules import ExprLexerRules, ID_REGEX, VARIABLE_NAME_REGEX
from feathub.dsl.expr_parser import ExprParser

lexer = lex.lex(module=ExprLexerRules())
_parser = ExprParser()


def get_variables(feathub_expr: str) -> Set[str]:
    """
    Get the variables in the given Feathub expression.
    """
    lexer.input(feathub_expr)
    variables = set()
    while True:
        tok = lexer.token()
        if not tok:
            break
        if tok.type == "ID" and tok.value not in BUILTIN_FUNC_DEF_MAP:
            variables.add(tok.value)
    return variables


def is_id(expr: str) -> bool:
    """
    Returns whether the feature expression is a single ID.
    """
    match_result = re.match(ID_REGEX, expr)
    return match_result is not None and match_result.end() == len(expr)


def get_var_name(expr: str) -> str:
    """
    Returns the variable name from the feature expression of the
    expression is a single ID.
    """
    return re.search(VARIABLE_NAME_REGEX, expr)[0]


def is_static_map_lookup_op(expr: str) -> bool:
    """
    Returns whether the feature expression represents a static lookup
    operation on a map-typed variable. "Static" means the lookup key
    is a literal instead of a variable.
    """
    ast = _parser.parse(expr)
    return (
        isinstance(ast, BracketOp)
        and isinstance(ast.left_child, VariableNode)
        and isinstance(ast.right_child, ValueNode)
    )


def get_static_map_lookup_variable_and_key(expr: str) -> Tuple[str, Any]:
    """
    Get the map-typed variable name and the static lookup key if the
    expression is a static map lookup operation.
    """
    ast = _parser.parse(expr)
    return (
        cast(VariableNode, cast(BracketOp, ast).left_child).var_name,
        cast(ValueNode, cast(BracketOp, ast).right_child).value,
    )

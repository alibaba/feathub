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
from typing import Set

from ply import lex

from feathub.dsl.built_in_func import BUILTIN_FUNC_DEF_MAP
from feathub.dsl.expr_lexer_rules import ExprLexerRules

lexer = lex.lex(module=ExprLexerRules())


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

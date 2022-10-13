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
from typing import Any, Dict, Optional

from feathub.dsl.abstract_ast_evaluator import AbstractAstEvaluator
from feathub.dsl.ast import (
    ArgListNode,
    VariableNode,
    FuncCallOp,
    ValueNode,
    CompareOp,
    UminusOp,
    BinaryOp,
    GroupNode,
)
from feathub.processors.local.ast_evaluator.functions import get_predefined_function


class LocalAstEvaluator(AbstractAstEvaluator):
    """
    AST Evaluator for local processor.
    """

    def eval_binary_op(self, ast: BinaryOp, variables: Optional[Dict]) -> Any:
        left_value = self.eval(ast.left_child, variables)
        right_value = self.eval(ast.right_child, variables)

        if ast.op_type == "+":
            return left_value + right_value
        elif ast.op_type == "-":
            return left_value - right_value
        elif ast.op_type == "*":
            return left_value * right_value
        elif ast.op_type == "/":
            return left_value / right_value
        else:
            raise RuntimeError(f"Unsupported op type: {ast.op_type}.")

    def eval_uminus_op(self, ast: UminusOp, variables: Optional[Dict]) -> Any:
        child_value = self.eval(ast.child, variables)
        return -child_value

    def eval_compare_op(self, ast: CompareOp, variables: Optional[Dict]) -> Any:
        left_value = self.eval(ast.left_child, variables)
        right_value = self.eval(ast.right_child, variables)

        if ast.op_type == "<":
            return left_value < right_value
        elif ast.op_type == "<=":
            return left_value <= right_value
        elif ast.op_type == ">":
            return left_value > right_value
        elif ast.op_type == ">=":
            return left_value >= right_value
        elif ast.op_type == "==":
            return left_value == right_value
        elif ast.op_type == "<>":
            return left_value != right_value
        else:
            raise RuntimeError(f"Unsupported op type: {ast.op_type}.")

    def eval_value_node(self, ast: ValueNode, variables: Optional[Dict]) -> Any:
        return ast.value

    def eval_func_call_op(self, ast: FuncCallOp, variables: Optional[Dict]) -> Any:
        values = self.eval(ast.args, variables)
        func = get_predefined_function(ast.func_name)
        if func is not None:
            return func(*values)
        raise RuntimeError(f"Unsupported function: {ast.func_name}.")

    def eval_variable_node(self, ast: VariableNode, variables: Optional[Dict]) -> Any:
        if ast.var_name not in variables:
            raise RuntimeError(
                f"Variable '{ast.var_name}' is not found in {variables}."
            )

        return variables[ast.var_name]

    def eval_arglist_node(self, ast: ArgListNode, variables: Optional[Dict]) -> Any:
        return [self.eval(value, variables) for value in ast.values]

    def eval_group_node(self, ast: GroupNode, variables: Optional[Dict]) -> Any:
        return self.eval(ast.child, variables)

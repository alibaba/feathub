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
from typing import Optional, Dict, Any

from feathub.dsl.abstract_ast_evaluator import AbstractAstEvaluator
from feathub.dsl.ast import (
    LogicalOp,
    CastOp,
    ArgListNode,
    VariableNode,
    FuncCallOp,
    ValueNode,
    CompareOp,
    UminusOp,
    BinaryOp,
    GroupNode,
    IsOp,
    NullNode,
    CaseOp,
)
from feathub.processors.spark.ast_evaluator.functions import evaluate_function


class SparkAstEvaluator(AbstractAstEvaluator):
    """
    AST Evaluator for Spark processor.

    The result is the Spark SQL expression string.
    """

    def eval_binary_op(self, ast: BinaryOp, variables: Optional[Dict]) -> Any:
        left_val = self.eval(ast.left_child, variables)
        right_val = self.eval(ast.right_child, variables)
        return f"{left_val} {ast.op_type} {right_val}"

    def eval_uminus_op(self, ast: UminusOp, variables: Optional[Dict]) -> Any:
        return f"-{self.eval(ast.child, variables)}"

    def eval_compare_op(self, ast: CompareOp, variables: Optional[Dict]) -> Any:
        left_val = self.eval(ast.left_child, variables)
        right_val = self.eval(ast.right_child, variables)
        return f"{left_val} {ast.op_type} {right_val}"

    def eval_value_node(self, ast: ValueNode, variables: Optional[Dict]) -> Any:
        if isinstance(ast.value, str):
            return f"'{ast.value}'"
        if isinstance(ast.value, bool):
            return str(ast.value).lower()
        return str(ast.value)

    def eval_func_call_op(self, ast: FuncCallOp, variables: Optional[Dict]) -> Any:
        args = [self.eval(v, variables) for v in ast.args.values]
        return evaluate_function(ast.func_name, args)

    def eval_variable_node(self, ast: VariableNode, variables: Optional[Dict]) -> Any:
        return f"`{ast.var_name}`"

    def eval_arglist_node(self, ast: ArgListNode, variables: Optional[Dict]) -> Any:
        return ", ".join([self.eval(value, variables) for value in ast.values])

    def eval_cast_node(self, ast: CastOp, variables: Optional[Dict]) -> Any:
        if ast.exception_on_failure:
            return f"CAST({self.eval(ast.child, variables)} AS {ast.type_name})"
        # TODO: Add document explaining that TRY_CAST is not available in community
        #  version of Spark, and code to check the availability of the UDFs.
        return f"TRY_CAST({self.eval(ast.child, variables)} AS {ast.type_name})"

    def eval_logical_op(self, ast: LogicalOp, variables: Optional[Dict]) -> Any:
        left_val = self.eval(ast.left_child, variables)
        right_val = self.eval(ast.right_child, variables)
        return f"{left_val} {ast.op_type} {right_val}"

    def eval_group_node(self, ast: GroupNode, variables: Optional[Dict]) -> Any:
        return f"({self.eval(ast.child, variables)})"

    def eval_is_op(self, ast: IsOp, variables: Optional[Dict]) -> Any:
        raise RuntimeError("IS/IS NOT operation is not supported.")

    def eval_null_node(self, ast: NullNode, variables: Optional[Dict]) -> Any:
        raise RuntimeError("NULL operation is not supported.")

    def eval_case_op(self, ast: CaseOp, variables: Optional[Dict]) -> Any:
        raise RuntimeError("CASE operation is not supported.")

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
import abc
from abc import ABC
from typing import Any, Optional, Dict

from feathub.common.exceptions import FeathubExpressionException
from feathub.dsl.ast import (
    ExprAST,
    BinaryOp,
    UminusOp,
    CompareOp,
    ValueNode,
    FuncCallOp,
    VariableNode,
    ArgListNode,
    CastOp,
    LogicalOp,
    GroupNode,
)


class AbstractAstEvaluator(ABC):
    """
    AbstractAstEvaluator is the base class for AST evaluators for different processors.
    """

    def eval(self, ast: ExprAST, variables: Optional[Dict]) -> Any:
        """
        Evaluate the AST with the given variables.

        :param ast: The root of the AST.
        :param variables: Map from variable name to its value.
        :return: The result of evaluate the AST with the given variables.
        """

        if isinstance(ast, BinaryOp):
            return self.eval_binary_op(ast, variables)
        if isinstance(ast, UminusOp):
            return self.eval_uminus_op(ast, variables)
        if isinstance(ast, CompareOp):
            return self.eval_compare_op(ast, variables)
        if isinstance(ast, ValueNode):
            return self.eval_value_node(ast, variables)
        if isinstance(ast, FuncCallOp):
            return self.eval_func_call_op(ast, variables)
        if isinstance(ast, VariableNode):
            return self.eval_variable_node(ast, variables)
        if isinstance(ast, ArgListNode):
            return self.eval_arglist_node(ast, variables)
        if isinstance(ast, CastOp):
            return self.eval_cast_node(ast, variables)
        if isinstance(ast, LogicalOp):
            return self.eval_logical_op(ast, variables)
        if isinstance(ast, GroupNode):
            return self.eval_group_node(ast, variables)

        raise FeathubExpressionException(f"Unknown AST node {type(ast)}.")

    @abc.abstractmethod
    def eval_binary_op(self, ast: BinaryOp, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_uminus_op(self, ast: UminusOp, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_compare_op(self, ast: CompareOp, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_value_node(self, ast: ValueNode, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_func_call_op(self, ast: FuncCallOp, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_variable_node(self, ast: VariableNode, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_arglist_node(self, ast: ArgListNode, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_cast_node(self, ast: CastOp, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_logical_op(self, ast: LogicalOp, variables: Optional[Dict]) -> Any:
        pass

    @abc.abstractmethod
    def eval_group_node(self, ast: GroupNode, variables: Optional[Dict]) -> Any:
        pass

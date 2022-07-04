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

import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from feathub.dsl.functions import get_predefined_function


class ExprAST(ABC):
    def __init__(self, node_type: str) -> None:
        self.node_type = node_type

    @abstractmethod
    def eval(self, variables: Dict) -> Any:
        """
        :param variables: A dict that maps variable name to value.
        :return: A value representing the evaluation result.
        """
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        """
        Returns a json-formatted object representing this node.
        """
        pass

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)


class BinaryOp(ExprAST):
    def __init__(self, op_type: str, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(node_type="BinaryOp")
        self.op_type = op_type
        self.left_child = left_child
        self.right_child = right_child

    def eval(self, variables: Dict) -> Any:
        left_value = self.left_child.eval(variables)
        right_value = self.right_child.eval(variables)

        if self.op_type == "+":
            return left_value + right_value
        elif self.op_type == "-":
            return left_value - right_value
        elif self.op_type == "*":
            return left_value * right_value
        elif self.op_type == "/":
            return left_value / right_value
        else:
            raise RuntimeError(f"Unsupported op type: {self.op_type}.")

    def to_json(self) -> Dict:
        return {
            "node_type": "BinaryOp",
            "op_type": self.op_type,
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class CompareOp(ExprAST):
    def __init__(self, op_type: str, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(node_type="CompareOp")
        self.op_type = op_type
        self.left_child = left_child
        self.right_child = right_child

    def eval(self, variables: Dict) -> Any:
        left_value = self.left_child.eval(variables)
        right_value = self.right_child.eval(variables)

        if self.op_type == "<":
            return left_value < right_value
        elif self.op_type == "<=":
            return left_value <= right_value
        elif self.op_type == ">":
            return left_value > right_value
        elif self.op_type == ">=":
            return left_value >= right_value
        elif self.op_type == "==":
            return left_value == right_value
        elif self.op_type == "<>":
            return left_value != right_value
        else:
            raise RuntimeError(f"Unsupported op type: {self.op_type}.")

    def to_json(self) -> Dict:
        return {
            "node_type": "CompareOp",
            "op_type": self.op_type,
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class UminusOp(ExprAST):
    def __init__(self, child: ExprAST) -> None:
        super().__init__(node_type="UminusNode")
        self.child = child

    def eval(self, variables: Dict) -> Any:
        child_value = self.child.eval(variables)
        return -child_value

    def to_json(self) -> Dict:
        return {
            "node_type": "UminusNode",
            "child": self.child,
        }


class ValueNode(ExprAST):
    def __init__(self, value: Any) -> None:
        super().__init__(node_type="ValueNode")
        self.value = value

    def eval(self, variables: Dict) -> Any:
        return self.value

    def to_json(self) -> Dict:
        return {
            "node_type": "ValueNode",
            "value": self.value,
        }


class VariableNode(ExprAST):
    def __init__(self, var_name: str) -> None:
        super().__init__(node_type="VariableNode")
        self.var_name = var_name

    def eval(self, variables: Dict) -> Any:
        if self.var_name not in variables:
            raise RuntimeError(
                f"Variable '{self.var_name}' is not found in {variables}."
            )

        return variables[self.var_name]

    def to_json(self) -> Dict:
        return {
            "node_type": "VariableNode",
            "var_name": self.var_name,
        }


class ArgListNode(ExprAST):
    def __init__(self, values: List[ExprAST]) -> None:
        super().__init__(node_type="ArgList")
        self.values = values

    def eval(self, variables: Dict) -> Any:
        return [value.eval(variables) for value in self.values]

    def to_json(self) -> Dict:
        return {
            "node_type": "ArgList",
            "values": [value.to_json() for value in self.values],
        }


class FuncCallOp(ExprAST):
    def __init__(self, func_name: str, args: ArgListNode) -> None:
        super().__init__(node_type="FuncCall")
        self.func_name = func_name
        self.args = args

    def eval(self, variables: Dict) -> Any:
        values = self.args.eval(variables)
        func = get_predefined_function(self.func_name)
        if func is not None:
            return func(*values)
        raise RuntimeError(f"Unsupported function: {self.func_name}.")

    def to_json(self) -> Dict:

        return {
            "node_type": "FuncCall",
            "func_name": self.func_name,
            "args": self.args.to_json(),
        }

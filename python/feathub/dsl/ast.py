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
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from feathub.common.exceptions import FeathubException


class ExprAST(ABC):
    def __init__(self, node_type: str) -> None:
        self.node_type = node_type

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

    def to_json(self) -> Dict:
        return {
            "node_type": "CompareOp",
            "op_type": self.op_type,
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class UminusOp(ExprAST):
    def __init__(self, child: ExprAST) -> None:
        super().__init__(node_type="UminusOp")
        self.child = child

    def to_json(self) -> Dict:
        return {
            "node_type": "UminusOp",
            "child": self.child,
        }


class LogicalOp(ExprAST):
    def __init__(self, op_type: str, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(node_type="LogicalOp")
        self.op_type = op_type.upper()
        self.left_child = left_child
        self.right_child = right_child

    def to_json(self) -> Dict:
        return {
            "node_type": "LogicalOp",
            "op_type": self.op_type,
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class CastOp(ExprAST):
    def __init__(
        self, child: ExprAST, type_name: str, exception_on_failure: bool = True
    ):
        super().__init__(node_type="CastOp")
        self.child = child
        self.type_name = type_name
        self.exception_on_failure = exception_on_failure

    def to_json(self) -> Dict:
        return {
            "node_type": "CastOp",
            "child": self.child.to_json(),
            "type_name": self.type_name,
            "exception_on_failure": self.exception_on_failure,
        }


class ValueNode(ExprAST):
    def __init__(self, value: Any) -> None:
        super().__init__(node_type="ValueNode")
        self.value = value

    def to_json(self) -> Dict:
        return {
            "node_type": "ValueNode",
            "value": self.value,
        }


class VariableNode(ExprAST):
    def __init__(self, var_name: str) -> None:
        super().__init__(node_type="VariableNode")
        self.var_name = var_name

    def to_json(self) -> Dict:
        return {
            "node_type": "VariableNode",
            "var_name": self.var_name,
        }


class ArgListNode(ExprAST):
    def __init__(self, values: List[ExprAST]) -> None:
        super().__init__(node_type="ArgListNode")
        self.values = values

    def to_json(self) -> Dict:
        return {
            "node_type": "ArgListNode",
            "values": [value.to_json() for value in self.values],
        }


class FuncCallOp(ExprAST):
    def __init__(self, func_name: str, args: ArgListNode) -> None:
        super().__init__(node_type="FuncCallOp")
        self.func_name = func_name.upper()
        self.args = args

    def to_json(self) -> Dict:
        return {
            "node_type": "FuncCallOp",
            "func_name": self.func_name,
            "args": self.args.to_json(),
        }


class GroupNode(ExprAST):
    def __init__(self, child: ExprAST) -> None:
        super().__init__("GroupNode")
        self.child = child

    def to_json(self) -> Dict:
        return {"node_type": "GroupNode", "child": self.child}


class NullNode(ExprAST):
    def __init__(self) -> None:
        super().__init__(node_type="NullNode")

    def to_json(self) -> Dict:
        return {"node_type": "NullNode"}


class IsOp(ExprAST):
    def __init__(
        self, left_child: ExprAST, right_child: ExprAST, is_not: bool = False
    ) -> None:
        super().__init__(node_type="IsOp")
        if not isinstance(right_child, NullNode):
            raise FeathubException("IS/IS NOT can only be concatenated with NULL.")

        self.left_child = left_child
        self.is_not = is_not

    def to_json(self) -> Dict:
        return {
            "node_type": "IsOp",
            "left_child": self.left_child.to_json(),
            "is_not": self.is_not,
        }


class CaseOp(ExprAST):
    def __init__(
        self,
        conditions: List[ExprAST],
        results: List[ExprAST],
        default: ExprAST,
    ) -> None:
        super().__init__(node_type="CaseOp")

        if len(conditions) != len(results):
            raise FeathubException(
                "The number of conditions and results does not match."
            )

        if not conditions:
            raise FeathubException("Cannot create CaseOp without cases.")

        self.conditions = conditions
        self.results = results
        self.default = default

    def to_json(self) -> Dict:
        return {
            "node_type": "CaseOp",
            "conditions": [x.to_json() for x in self.conditions],
            "results": [x.to_json() for x in self.results],
            "default": self.default.to_json(),
        }

    @staticmethod
    def new_builder() -> "CaseOp.Builder":
        return CaseOp.Builder()

    class Builder:
        def __init__(self) -> None:
            self.conditions: List[ExprAST] = []
            self.results: List[ExprAST] = []
            self.default_value: ExprAST = NullNode()

        def case(self, condition: ExprAST, result: ExprAST) -> "CaseOp.Builder":
            self.conditions.append(condition)
            self.results.append(result)
            return self

        def default(self, result: ExprAST) -> "CaseOp.Builder":
            self.default_value = result
            return self

        def build(self) -> "CaseOp":
            return CaseOp(self.conditions, self.results, self.default_value)

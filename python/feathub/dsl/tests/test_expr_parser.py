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
import unittest

from feathub.dsl.ast import (
    FuncCallOp,
    ArgListNode,
    VariableNode,
    ValueNode,
    LogicalOp,
    IsOp,
    NullNode,
    CaseOp,
    CompareOp,
)
from feathub.dsl.expr_parser import ExprParser


class ExprParserTest(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = ExprParser()

    def test_expr_parser(self):
        expected_mappings = {
            "UNIX_TIMESTAMP(time)": FuncCallOp(
                func_name="UNIX_TIMESTAMP",
                args=ArgListNode(values=[VariableNode(var_name="time")]),
            ),
            "UNIX_TIMESTAMP(time, '%Y-%m-%d %H:%M:%S.%f %z')": FuncCallOp(
                func_name="UNIX_TIMESTAMP",
                args=ArgListNode(
                    values=[
                        VariableNode(var_name="time"),
                        ValueNode(value="%Y-%m-%d %H:%M:%S.%f %z"),
                    ]
                ),
            ),
            "a AND b": LogicalOp(
                op_type="AND",
                left_child=VariableNode(var_name="a"),
                right_child=VariableNode(var_name="b"),
            ),
            "a and b": LogicalOp(
                op_type="AND",
                left_child=VariableNode(var_name="a"),
                right_child=VariableNode(var_name="b"),
            ),
            "a IS NULL": IsOp(
                left_child=VariableNode(var_name="a"),
                right_child=NullNode(),
            ),
            "a IS NOT NULL": IsOp(
                left_child=VariableNode(var_name="a"),
                right_child=NullNode(),
                is_not=True,
            ),
            "CASE WHEN a > b THEN 1 WHEN a < b THEN 2 ELSE 3 END": CaseOp(
                conditions=[
                    CompareOp(
                        op_type=">",
                        left_child=VariableNode(var_name="a"),
                        right_child=VariableNode(var_name="b"),
                    ),
                    CompareOp(
                        op_type="<",
                        left_child=VariableNode(var_name="a"),
                        right_child=VariableNode(var_name="b"),
                    ),
                ],
                results=[
                    ValueNode(value=1),
                    ValueNode(value=2),
                ],
                default=ValueNode(value=3),
            ),
            "CASE WHEN a IS NULL THEN 1 END": CaseOp(
                conditions=[
                    IsOp(
                        left_child=VariableNode(var_name="a"),
                        right_child=NullNode(),
                    ),
                ],
                results=[ValueNode(value=1)],
                default=NullNode(),
            ),
        }

        for expr, node in expected_mappings.items():
            self.assertEqual(node.to_json(), self.parser.parse(expr).to_json())

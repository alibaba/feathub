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

from feathub.dsl.ast import FuncCallOp, ArgListNode, VariableNode, ValueNode
from feathub.dsl.expr_parser import ExprParser


class ExprParserTest(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = ExprParser()

    def test_expr_parser(self):
        expected_mappings = {
            "UNIX_TIMESTAMP(time)": FuncCallOp(
                func_name="UNIX_TIMESTAMP",
                args=ArgListNode(
                    values=[
                        VariableNode(
                            var_name="time",
                        )
                    ]
                ),
            ),
            "UNIX_TIMESTAMP(time, '%Y-%m-%d %H:%M:%S.%f %z')": FuncCallOp(
                func_name="UNIX_TIMESTAMP",
                args=ArgListNode(
                    values=[
                        VariableNode(
                            var_name="time",
                        ),
                        ValueNode(
                            value="%Y-%m-%d %H:%M:%S.%f %z",
                        ),
                    ]
                ),
            ),
        }

        for expr, node in expected_mappings.items():
            self.assertEqual(node.to_json(), self.parser.parse(expr).to_json())

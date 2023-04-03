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
import unittest

from feathub.dsl.expr_utils import get_variables


class ExprUtilsTest(unittest.TestCase):
    def test_get_variables(self):
        expr = """
        cast (a AS FLOAT) + CAST(b AS INTEGER) OR a = c AND `integer` <> `cast`
        """
        self.assertSetEqual({"a", "b", "c", "integer", "cast"}, get_variables(expr))

        expr = "UNIX_TIMESTAMP(a)"
        self.assertSetEqual({"a"}, get_variables(expr))

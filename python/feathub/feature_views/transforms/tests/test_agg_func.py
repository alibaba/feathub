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

from feathub.common.types import Int32, Float64, Int64, MapType
from feathub.feature_views.transforms.agg_func import AggFunc


class AggFuncTest(unittest.TestCase):
    def test_get_result_type(self):
        self.assertEqual(Int32, AggFunc.SUM.get_result_type(Int32))
        self.assertEqual(Int32, AggFunc.MIN.get_result_type(Int32))
        self.assertEqual(Int32, AggFunc.MAX.get_result_type(Int32))
        self.assertEqual(Int32, AggFunc.FIRST_VALUE.get_result_type(Int32))
        self.assertEqual(Int32, AggFunc.LAST_VALUE.get_result_type(Int32))
        self.assertEqual(Float64, AggFunc.AVG.get_result_type(Int32))
        self.assertEqual(Int64, AggFunc.ROW_NUMBER.get_result_type(Int32))
        self.assertEqual(Int64, AggFunc.COUNT.get_result_type(Int32))
        self.assertEqual(
            MapType(Int32, Int64), AggFunc.VALUE_COUNTS.get_result_type(Int32)
        )

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

from enum import Enum

from feathub.common.exceptions import FeathubException
from feathub.common.types import DType, Float64, Int64, MapType, VectorType


class AggFunc(Enum):
    """Supported aggregation function for over/sliding window transform."""

    AVG = "AVG"
    SUM = "SUM"
    MAX = "MAX"
    MIN = "MIN"
    FIRST_VALUE = "FIRST_VALUE"
    LAST_VALUE = "LAST_VALUE"
    ROW_NUMBER = "ROW_NUMBER"
    COUNT = "COUNT"
    VALUE_COUNTS = "VALUE_COUNTS"
    COLLECT_LIST = "COLLECT_LIST"

    def get_result_type(self, input_type: DType) -> DType:
        if self == AggFunc.AVG:
            return Float64
        elif (
            self == AggFunc.SUM
            or self == AggFunc.MAX
            or self == AggFunc.MIN
            or self == AggFunc.FIRST_VALUE
            or self == AggFunc.LAST_VALUE
        ):
            return input_type
        elif self == AggFunc.ROW_NUMBER or self == AggFunc.COUNT:
            return Int64
        elif self == AggFunc.VALUE_COUNTS:
            return MapType(input_type, Int64)
        elif self == AggFunc.COLLECT_LIST:
            return VectorType(input_type)

        raise FeathubException(f"Unknown AggFunc {self}.")

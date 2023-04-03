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
from typing import List, Callable

from feathub.common.exceptions import FeathubExpressionException
from feathub.common.types import DType, String, Int64


class BuiltInFuncDefinition:
    def __init__(self, name: str, result_type_strategy: Callable[[List[DType]], DType]):
        self.name = name
        self.result_type_strategy = result_type_strategy

    def get_result_type(self, input_types: List[DType]) -> DType:
        return self.result_type_strategy(input_types)


BUILTIN_FUNCS = [
    BuiltInFuncDefinition(
        name="LOWER", result_type_strategy=lambda input_types: String
    ),
    BuiltInFuncDefinition(
        name="UNIX_TIMESTAMP", result_type_strategy=lambda input_types: Int64
    ),
]

BUILTIN_FUNC_DEF_MAP = {f.name: f for f in BUILTIN_FUNCS}


def get_builtin_func_def(name: str) -> BuiltInFuncDefinition:
    if name not in BUILTIN_FUNC_DEF_MAP:
        raise FeathubExpressionException(f"Unknown function name: {name}.")
    return BUILTIN_FUNC_DEF_MAP[name]

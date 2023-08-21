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
from feathub.common.types import DType, String, Int64, MapType, VectorType


class BuiltInFuncDefinition:
    def __init__(self, name: str, result_type_strategy: Callable[[List[DType]], DType]):
        self.name = name
        self.result_type_strategy = result_type_strategy

    def get_result_type(self, input_types: List[DType]) -> DType:
        return self.result_type_strategy(input_types)


def map_type_strategy(input_types: List[DType]) -> DType:
    if len(input_types) < 2:
        raise FeathubExpressionException("Map requires at least 2 arguments.")
    value_type = None
    key_type = None
    for i in range(0, len(input_types), 2):
        if key_type is None:
            key_type = input_types[i]
        elif key_type != input_types[i]:
            raise FeathubExpressionException("Map keys must be the same type.")

        if value_type is None:
            value_type = input_types[i + 1]
        elif value_type != input_types[i + 1]:
            raise FeathubExpressionException("Map values must be the same type.")

    return MapType(key_type, value_type)


BUILTIN_FUNCS = [
    BuiltInFuncDefinition(
        name="LOWER", result_type_strategy=lambda input_types: String
    ),
    BuiltInFuncDefinition(
        name="CONCAT", result_type_strategy=lambda input_types: String
    ),
    BuiltInFuncDefinition(
        name="CONCAT_WS", result_type_strategy=lambda input_types: String
    ),
    BuiltInFuncDefinition(
        name="UNIX_TIMESTAMP", result_type_strategy=lambda input_types: Int64
    ),
    BuiltInFuncDefinition(
        name="JSON_STRING", result_type_strategy=lambda input_types: String
    ),
    BuiltInFuncDefinition(name="MAP", result_type_strategy=map_type_strategy),
    # TODO: Add test and document for ARRAY.
    BuiltInFuncDefinition(
        name="ARRAY",
        result_type_strategy=lambda input_types: VectorType(input_types[0]),
    ),
]

BUILTIN_FUNC_DEF_MAP = {f.name: f for f in BUILTIN_FUNCS}


def get_builtin_func_def(name: str) -> BuiltInFuncDefinition:
    if name not in BUILTIN_FUNC_DEF_MAP:
        raise FeathubExpressionException(f"Unknown function name: {name}.")
    return BUILTIN_FUNC_DEF_MAP[name]

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

from enum import Enum
from abc import ABC, abstractmethod
from typing import Type, Dict

import numpy as np
import json

from feathub.common.exceptions import FeathubTypeException, FeathubExpressionException


class BasicDType(Enum):
    """Basic value types."""

    UNKNOWN = 0
    BYTES = 1
    STRING = 2
    INT32 = 3
    INT64 = 4
    FLOAT64 = 5
    FLOAT32 = 6
    BOOL = 7
    TIMESTAMP = 8


class DType(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        pass

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and self.to_json() == other.to_json()

    def __hash__(self) -> int:
        return hash(((k, v) for k, v in self.to_json().items()))


class PrimitiveType(DType):
    def __init__(self, basic_dtype: BasicDType) -> None:
        super().__init__()
        self.basic_dtype = basic_dtype

    def to_json(self) -> Dict:
        return {"type": "PrimitiveType", "basic_dtype": f"{self.basic_dtype.name}"}


class VectorType(DType):
    def __init__(self, dtype: DType) -> None:
        super().__init__()
        self.dtype = dtype

    def to_json(self) -> Dict:
        return {"type": "VectorType", "dtype": self.dtype.to_json()}


class MapType(DType):
    def __init__(self, key_dtype: DType, value_dtype: DType) -> None:
        super().__init__()
        self.key_dtype = key_dtype
        self.value_dtype = value_dtype

    def to_json(self) -> Dict:
        return {
            "type": "MapType",
            "key_dtype": self.key_dtype.to_json(),
            "value_dtype": self.value_dtype.to_json(),
        }


def from_numpy_dtype(dtype: Type) -> DType:
    if dtype == np.str:
        return String
    elif dtype == np.bool:
        return Bool
    elif dtype == np.int32:
        return Int32
    elif dtype == np.int64:
        return Int64
    elif dtype == np.float:
        return Float32
    elif dtype == np.double:
        return Float64
    elif dtype == np.object:
        return Unknown

    raise FeathubTypeException(f"Unsupported numpy type {dtype}.")


def to_numpy_dtype(dtype: DType) -> Type:
    if dtype == String:
        return np.str
    elif dtype == Bool:
        return np.bool
    elif dtype == Int32:
        return np.int32
    elif dtype == Int64:
        return np.int64
    elif dtype == Float32:
        return np.float
    elif dtype == Float64:
        return np.double
    elif isinstance(dtype, MapType):
        return np.object
    elif dtype == Unknown:
        return np.object

    raise FeathubTypeException(f"Converting {dtype} to numpy type is not supported.")


Unknown = PrimitiveType(BasicDType.UNKNOWN)
Bytes = PrimitiveType(BasicDType.BYTES)
String = PrimitiveType(BasicDType.STRING)
Bool = PrimitiveType(BasicDType.BOOL)
Int32 = PrimitiveType(BasicDType.INT32)
Int64 = PrimitiveType(BasicDType.INT64)
Float32 = PrimitiveType(BasicDType.FLOAT32)
Float64 = PrimitiveType(BasicDType.FLOAT64)
Timestamp = PrimitiveType(BasicDType.TIMESTAMP)

name_to_dtype: Dict[str, DType] = {
    "BYTES": Bytes,
    "STRING": String,
    "INTEGER": Int32,
    "BIGINT": Int64,
    "FLOAT": Float32,
    "DOUBLE": Float64,
    "BOOLEAN": Bool,
    "TIMESTAMP": Timestamp,
}


def get_type_by_name(type_name: str) -> DType:
    if type_name not in name_to_dtype:
        raise FeathubExpressionException(f"Unknown dtype name: {type_name}.")
    return name_to_dtype[type_name]


python_type_to_dtype: Dict[Type, DType] = {
    bool: Bool,
    int: Int32,
    float: Float64,
    str: String,
}


def from_python_type(python_type: Type) -> DType:
    if python_type not in python_type_to_dtype:
        raise FeathubExpressionException(
            f"Cannot convert python type: {python_type} to Feathub dtype."
        )
    return python_type_to_dtype[python_type]


Int32Vector = VectorType(Int32)
Int64Vector = VectorType(Int64)
Float32Vector = VectorType(Float32)
Float64Vector = VectorType(Float64)

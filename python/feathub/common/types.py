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
import numpy as np
import json


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
    UNIX_TIMESTAMP = 8


class DType(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def to_json(self):
        pass

    def __str__(self):
        return json.dumps(self.to_json(), indent=2, sort_keys=True)


class PrimitiveType(DType):
    def __init__(self, basic_dtype: BasicDType):
        super().__init__()
        self.basic_dtype = basic_dtype

    def to_json(self):
        return f"{self.basic_dtype.name}"


class VectorType(DType):
    def __init__(self, basic_dtype: BasicDType):
        super().__init__()
        self.basic_dtype = basic_dtype

    def to_json(self):
        return f"VectorType({self.basic_dtype.name})"


def from_numpy_dtype(dtype: np.dtype) -> DType:
    if dtype == np.str:
        return String
    elif dtype == np.bool:
        return Bool
    elif dtype == np.int:
        return Int32
    elif dtype == np.long:
        return Int64
    elif dtype == np.float:
        return Float32
    elif dtype == np.double:
        return Float64
    elif dtype == np.object:
        return Unknown

    raise RuntimeError(f"Unsupported numpy type {dtype}.")


Unknown = PrimitiveType(BasicDType.UNKNOWN)
Bytes = PrimitiveType(BasicDType.BYTES)
String = PrimitiveType(BasicDType.STRING)
Bool = PrimitiveType(BasicDType.BOOL)
Int32 = PrimitiveType(BasicDType.INT32)
Int64 = PrimitiveType(BasicDType.INT64)
Float32 = PrimitiveType(BasicDType.FLOAT32)
Float64 = PrimitiveType(BasicDType.FLOAT64)
UnixTimestamp = PrimitiveType(BasicDType.UNIX_TIMESTAMP)

Int32Vector = VectorType(BasicDType.INT32)
Int64Vector = VectorType(BasicDType.INT64)
Float32Vector = VectorType(BasicDType.FLOAT32)
Float64Vector = VectorType(BasicDType.FLOAT64)

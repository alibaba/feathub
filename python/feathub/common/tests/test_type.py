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
from typing import cast

import numpy as np

from feathub.common import types
from feathub.common.exceptions import FeathubTypeException
from feathub.common.types import to_numpy_dtype, from_numpy_dtype


class TypesTest(unittest.TestCase):
    def test_numpy_type_conversion(self):
        datatypes = [
            types.String,
            types.Int32,
            types.Int64,
            types.Float32,
            types.Float64,
            types.String,
            types.Unknown,
            types.Bool,
        ]

        for dtype in datatypes:
            self.assertEquals(dtype, from_numpy_dtype(to_numpy_dtype(dtype)))

    def test_converting_unsupported_type(self):
        with self.assertRaises(FeathubTypeException):
            to_numpy_dtype(types.Timestamp)

        with self.assertRaises(FeathubTypeException):
            from_numpy_dtype(np.int16)

    def test_multi_level_composite_type(self):
        key_type = types.VectorType(types.Int64)
        element_type = types.MapType(key_type, types.Int64)
        dtype = types.VectorType(element_type)

        self.assertEqual(element_type, dtype.dtype)
        self.assertEqual(key_type, cast(types.MapType, dtype.dtype).key_dtype)
        self.assertEqual(types.Int64, cast(types.MapType, dtype.dtype).value_dtype)

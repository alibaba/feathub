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
from typing import Dict, Type

from pyspark.sql import types as native_spark_types

from feathub.common.exceptions import FeathubTypeException
from feathub.common import types
from feathub.table.schema import Schema

type_mapping: Dict[types.BasicDType, native_spark_types.AtomicType] = {
    types.BasicDType.BYTES: native_spark_types.BinaryType(),
    types.BasicDType.STRING: native_spark_types.StringType(),
    types.BasicDType.INT32: native_spark_types.IntegerType(),
    types.BasicDType.INT64: native_spark_types.LongType(),
    types.BasicDType.FLOAT32: native_spark_types.FloatType(),
    types.BasicDType.FLOAT64: native_spark_types.DoubleType(),
    types.BasicDType.BOOL: native_spark_types.BooleanType(),
    types.BasicDType.TIMESTAMP: native_spark_types.TimestampType(),
}

inverse_type_mapping: Dict[Type[native_spark_types.AtomicType], types.BasicDType] = {
    type(atomic_type): basic_type for basic_type, atomic_type in type_mapping.items()
}


def to_spark_struct_type(schema: Schema) -> native_spark_types.StructType:
    """
    Convert Feathub schema to native Spark StructType.

    :param schema: The Feathub schema.
    :return: The native Spark StructType.
    """

    fields = [
        native_spark_types.StructField(field_name, to_spark_type(field_type))
        for field_name, field_type in zip(schema.field_names, schema.field_types)
    ]

    return native_spark_types.StructType(fields)


def to_feathub_schema(struct_type: native_spark_types.StructType) -> Schema:
    """
    Convert Spark StructType to Feathub schema.

    :param struct_type: The Spark StructType.
    :return: The Feathub schema.
    """

    field_names = []
    field_types = []
    for field in struct_type.fields:
        field_names.append(field.name)
        field_types.append(to_feathub_type(field.dataType))
    return Schema(field_names, field_types)


def to_spark_type(feathub_type: types.DType) -> native_spark_types.DataType:
    """
    Convert Feathub DType to Spark DataType.

    :param feathub_type: The Feathub DType.
    :return: The Spark DataType.
    """
    if isinstance(feathub_type, types.BasicDType):
        return _basic_type_to_spark_type(feathub_type)
    elif isinstance(feathub_type, types.PrimitiveType):
        return _primitive_type_to_spark_type(feathub_type)
    elif isinstance(feathub_type, types.VectorType):
        return _vector_type_to_spark_type(feathub_type)
    elif isinstance(feathub_type, types.MapType):
        return _map_type_to_spark_type(feathub_type)

    raise FeathubTypeException(
        f"Type {feathub_type} is not supported by SparkProcessor."
    )


def to_feathub_type(spark_type: native_spark_types.DataType) -> types.DType:
    """
    Convert Spark DataType to Feathub DType.

    :param spark_type: The Spark DataType.
    :return: The Feathub DType.
    """
    if isinstance(spark_type, native_spark_types.AtomicType):
        return _atomic_type_to_feathub_type(spark_type)
    elif isinstance(spark_type, native_spark_types.ArrayType):
        return _array_type_to_feathub_type(spark_type)
    elif isinstance(spark_type, native_spark_types.MapType):
        return _map_type_to_feathub_type(spark_type)

    raise FeathubTypeException(f"Spark type {spark_type} is not supported by Feathub.")


def _primitive_type_to_spark_type(
    feathub_type: types.PrimitiveType,
) -> native_spark_types.DataType:
    return _basic_type_to_spark_type(feathub_type.basic_dtype)


def _vector_type_to_spark_type(
    feathub_type: types.VectorType,
) -> native_spark_types.DataType:
    return native_spark_types.ArrayType(to_spark_type(feathub_type.dtype))


def _map_type_to_spark_type(feathub_type: types.MapType) -> native_spark_types.MapType:
    return native_spark_types.MapType(
        to_spark_type(feathub_type.key_dtype),
        to_spark_type(feathub_type.value_dtype),
    )


def _basic_type_to_spark_type(
    basic_type: types.BasicDType,
) -> native_spark_types.AtomicType:
    if basic_type not in type_mapping:
        raise FeathubTypeException(
            f"Type {basic_type} is not supported by SparkProcessor."
        )
    return type_mapping[basic_type]


def _atomic_type_to_feathub_type(
    spark_type: native_spark_types.AtomicType,
) -> types.DType:
    return types.PrimitiveType(_atomic_type_to_basic_type(spark_type))


def _array_type_to_feathub_type(
    spark_type: native_spark_types.ArrayType,
) -> types.DType:
    element_type = spark_type.elementType
    if not isinstance(element_type, native_spark_types.AtomicType):
        raise FeathubTypeException(f"Unexpected element type {element_type}.")
    return types.VectorType(to_feathub_type(element_type))


def _atomic_type_to_basic_type(
    spark_type: native_spark_types.AtomicType,
) -> types.BasicDType:
    if type(spark_type) not in inverse_type_mapping:
        raise FeathubTypeException(
            f"Spark type {spark_type} is not supported by Feathub."
        )
    return inverse_type_mapping[type(spark_type)]


def _map_type_to_feathub_type(spark_type: native_spark_types.MapType) -> types.DType:
    return types.MapType(
        to_feathub_type(spark_type.keyType), to_feathub_type(spark_type.valueType)
    )

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
from typing import Dict, Type

from pyflink.table import DataTypes, Schema as NativeFlinkSchema, TableSchema
from pyflink.table.types import (
    DataType,
    AtomicType,
    ArrayType,
    MapType as NativeFlinkMapType,
)

from feathub.common.exceptions import FeathubTypeException
from feathub.common.types import DType, PrimitiveType, VectorType, BasicDType, MapType
from feathub.table.schema import Schema

type_mapping: Dict[BasicDType, AtomicType] = {
    BasicDType.BYTES: DataTypes.BYTES(),
    BasicDType.STRING: DataTypes.STRING(),
    BasicDType.INT32: DataTypes.INT(),
    BasicDType.INT64: DataTypes.BIGINT(),
    BasicDType.FLOAT32: DataTypes.FLOAT(),
    BasicDType.FLOAT64: DataTypes.DOUBLE(),
    BasicDType.BOOL: DataTypes.BOOLEAN(),
    BasicDType.TIMESTAMP: DataTypes.TIMESTAMP(3),
}

inverse_type_mapping: Dict[Type[AtomicType], BasicDType] = {
    type(atomic_type): basic_type for basic_type, atomic_type in type_mapping.items()
}


def to_flink_schema(schema: Schema) -> NativeFlinkSchema:
    """
    Convert Feathub schema to native Flink schema.

    :param schema: The Feathub schema.
    :return: The native Flink schema.
    """
    builder = NativeFlinkSchema.new_builder()
    for field_name, field_type in zip(schema.field_names, schema.field_types):
        builder.column(field_name, to_flink_type(field_type))

    return builder.build()


def to_feathub_schema(schema: TableSchema) -> Schema:
    """
    Convert Flink TableSchema to Feathub schema.

    :param schema: The Flink TableSchema.
    :return: The Feathub schema.
    """
    field_names = schema.get_field_names()
    field_types = [
        to_feathub_type(schema.get_field_data_type(field_name))
        for field_name in field_names
    ]
    return Schema(field_names, field_types)


def to_flink_type(feathub_type: DType) -> DataType:
    """
    Convert Feathub DType to Flink DataType.

    :param feathub_type: The Feathub Dtype.
    :return: The Flink DataType.
    """
    if isinstance(feathub_type, BasicDType):
        return _basic_type_to_flink_type(feathub_type)
    elif isinstance(feathub_type, PrimitiveType):
        return _primitive_type_to_flink_type(feathub_type)
    elif isinstance(feathub_type, VectorType):
        return _vector_type_to_flink_type(feathub_type)
    elif isinstance(feathub_type, MapType):
        return _map_type_to_flink_type(feathub_type)

    raise FeathubTypeException(
        f"Type {feathub_type} is not supported by FlinkProcessor."
    )


def to_feathub_type(flink_type: DataType) -> DType:
    """
    Convert Flink DataType to Feathub DType.

    :param flink_type: The Flink DataType.
    :return: The Feathub DType.
    """
    if isinstance(flink_type, AtomicType):
        return _atomic_type_to_feathub_type(flink_type)
    elif isinstance(flink_type, ArrayType):
        return _array_type_to_feathub_type(flink_type)
    elif isinstance(flink_type, NativeFlinkMapType):
        return _map_type_to_feathub_type(flink_type)

    raise FeathubTypeException(f"Flink type {flink_type} is not supported by Feathub.")


def _primitive_type_to_flink_type(feathub_type: PrimitiveType) -> DataType:
    return _basic_type_to_flink_type(feathub_type.basic_dtype)


def _vector_type_to_flink_type(feathub_type: VectorType) -> DataType:
    return DataTypes.ARRAY(to_flink_type(feathub_type.dtype))


def _map_type_to_flink_type(feathub_type: MapType) -> NativeFlinkMapType:
    return DataTypes.MAP(
        to_flink_type(feathub_type.key_dtype),
        to_flink_type(feathub_type.value_dtype),
    )


def _basic_type_to_flink_type(basic_type: BasicDType) -> AtomicType:
    if basic_type not in type_mapping:
        raise FeathubTypeException(
            f"Type {basic_type} is not supported by FlinkProcessor."
        )
    return type_mapping[basic_type]


def _atomic_type_to_feathub_type(flink_type: AtomicType) -> DType:
    return PrimitiveType(_atomic_type_to_basic_type(flink_type))


def _array_type_to_feathub_type(flink_type: ArrayType) -> DType:
    element_type = flink_type.element_type
    if not isinstance(element_type, AtomicType):
        raise FeathubTypeException(f"Unexpected element type {element_type}.")
    return VectorType(to_feathub_type(element_type))


def _atomic_type_to_basic_type(flink_type: AtomicType) -> BasicDType:
    if type(flink_type) not in inverse_type_mapping:
        raise FeathubTypeException(
            f"Flink type {flink_type} is not supported by Feathub."
        )
    return inverse_type_mapping[type(flink_type)]


def _map_type_to_feathub_type(flink_type: NativeFlinkMapType) -> DType:
    return MapType(
        to_feathub_type(flink_type.key_type), to_feathub_type(flink_type.value_type)
    )

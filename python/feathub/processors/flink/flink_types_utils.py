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
from typing import Dict, Type, Union

from pyflink.table import (
    DataTypes,
    Schema as NativeFlinkSchema,
    TableSchema,
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
)
from pyflink.table.types import (
    DataType,
    AtomicType,
    ArrayType,
    MapType as NativeFlinkMapType,
    VarCharType,
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
    Convert FeatHub schema to native Flink schema.

    :param schema: The FeatHub schema.
    :return: The native Flink schema.
    """
    builder = NativeFlinkSchema.new_builder()
    for field_name, field_type in zip(schema.field_names, schema.field_types):
        builder.column(field_name, to_flink_type(field_type))

    return builder.build()


def to_feathub_schema(schema: TableSchema) -> Schema:
    """
    Convert Flink TableSchema to FeatHub schema.

    :param schema: The Flink TableSchema.
    :return: The FeatHub schema.
    """
    field_names = schema.get_field_names()
    field_types = [
        to_feathub_type(schema.get_field_data_type(field_name))
        for field_name in field_names
    ]
    return Schema(field_names, field_types)


def to_flink_type(feathub_type: DType) -> DataType:
    """
    Convert FeatHub DType to Flink DataType.

    :param feathub_type: The FeatHub Dtype.
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


def cast_field_type_without_changing_nullability(
    table: NativeFlinkTable, field_name_and_types: Dict[str, DataType]
) -> NativeFlinkTable:
    """
    Converts given fields in the flink table to the dedicated data types.

    Note that this method would not change the nullability of the fields.

    :param table: the Flink table containing the fields to be casted
    :param field_name_and_types: a dict containing the names of the fields and the
                                 target data types to cast.
    """
    expressions = []
    for field_name, field_type in field_name_and_types.items():
        current_field_type = table.get_schema().get_field_data_type(field_name)
        field_type = _get_data_type_with_same_nullability(
            field_type, current_field_type
        )
        if field_type == current_field_type:
            continue
        expressions.append(
            native_flink_expr.col(field_name).cast(field_type).alias(field_name)
        )

    if not expressions:
        return table
    return table.add_or_replace_columns(*expressions)


def _get_data_type_with_same_nullability(
    original_type: DataType, reference_type: DataType
) -> DataType:
    """
    Recursively updates the nullability of the original type to be same as the
    reference type. Original type and reference type should be both atomic type
    or the same collection type.

    Example:

    (INT, BIGINT NOT NULL) -> INT NOT NULL

    (MAP<STRING NOT NULL, FLOAT NOT NULL>, MAP<STRING NOT NULL, DOUBLE> NOT NULL)
    -> MAP<STRING NOT NULL, FLOAT> NOT NULL

    :param original_type: DataType whose nullability would be updated.
    :param reference_type: DataType containing the target nullability.
    :return: original_type with its nullability aligned with reference_type.
    """
    if reference_type == original_type:
        return reference_type

    if isinstance(reference_type, AtomicType) and isinstance(original_type, AtomicType):
        return (
            original_type.nullable()
            if _is_nullable_flink_type(reference_type)
            else original_type.not_null()
        )
    elif isinstance(reference_type, ArrayType) and isinstance(original_type, ArrayType):
        element_type = _get_data_type_with_same_nullability(
            original_type.element_type, reference_type.element_type
        )
        return ArrayType(element_type, nullable=_is_nullable_flink_type(reference_type))
    elif isinstance(reference_type, NativeFlinkMapType) and isinstance(
        original_type, NativeFlinkMapType
    ):
        key_type = _get_data_type_with_same_nullability(
            original_type.key_type, reference_type.key_type
        )
        value_type = _get_data_type_with_same_nullability(
            original_type.value_type, reference_type.value_type
        )
        return NativeFlinkMapType(
            key_type, value_type, nullable=_is_nullable_flink_type(reference_type)
        )

    raise FeathubTypeException(
        f"Flink type combination {original_type} and {reference_type} "
        f"is not supported by FeatHub."
    )


def to_flink_sql_type(input_type: Union[DType, DataType]) -> str:
    """
    Convert FeatHub DType or Flink DataType to Flink SQL data type.

    :param input_type: The FeatHub Dtype or Flink DataType
    :return: The Flink SQL data type.
    """
    if isinstance(input_type, DType):
        flink_type = to_flink_type(input_type)
    else:
        flink_type = input_type

    if isinstance(flink_type, VarCharType):
        if flink_type.length == DataTypes.STRING().length:
            return "STRING"
        return f"VARCHAR({flink_type.length})"
    elif isinstance(flink_type, ArrayType):
        return f"ARRAY<{to_flink_sql_type(flink_type.element_type)}>"
    elif isinstance(flink_type, NativeFlinkMapType):
        return (
            f"MAP<{to_flink_sql_type(flink_type.key_type)}, "
            f"{to_flink_sql_type(flink_type.value_type)}>"
        )
    return str(flink_type)


def to_feathub_type(flink_type: DataType) -> DType:
    """
    Convert Flink DataType to FeatHub DType.

    :param flink_type: The Flink DataType.
    :return: The FeatHub DType.
    """
    if isinstance(flink_type, AtomicType):
        return _atomic_type_to_feathub_type(flink_type)
    elif isinstance(flink_type, ArrayType):
        return _array_type_to_feathub_type(flink_type)
    elif isinstance(flink_type, NativeFlinkMapType):
        return _map_type_to_feathub_type(flink_type)

    raise FeathubTypeException(f"Flink type {flink_type} is not supported by FeatHub.")


def _is_nullable_flink_type(flink_type: DataType) -> bool:
    return flink_type.nullable() == flink_type


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
            f"Flink type {flink_type} is not supported by FeatHub."
        )
    return inverse_type_mapping[type(flink_type)]


def _map_type_to_feathub_type(flink_type: NativeFlinkMapType) -> DType:
    return MapType(
        to_feathub_type(flink_type.key_type), to_feathub_type(flink_type.value_type)
    )

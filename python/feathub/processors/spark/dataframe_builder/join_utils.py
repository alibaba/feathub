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
from typing import Sequence, Optional

from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.spark.spark_types_utils import to_spark_type
from feathub.table.table_descriptor import TableDescriptor
from pyspark.sql import (
    DataFrame as NativeSparkDataFrame,
    functions,
    Window,
)
from pyspark.sql.types import DataType


class JoinFieldDescriptor:
    """
    Descriptor of the join field.
    """

    def __init__(
        self,
        field_name: str,
        field_data_type: Optional[DataType] = None,
    ):
        """
        :param field_name: The name of the field to join.
        :param field_data_type: Optional. If it is not None, the field is cast to the
                                given type. Otherwise, use its original type.
        """
        self.field_name = field_name
        self.field_data_type = field_data_type

    @staticmethod
    def from_table_descriptor_and_field_name(
        table_descriptor: TableDescriptor, field_name: str
    ) -> "JoinFieldDescriptor":
        feature = table_descriptor.get_feature(field_name)
        return JoinFieldDescriptor(field_name, to_spark_type(feature.dtype))

    @staticmethod
    def from_field_name(field_name: str) -> "JoinFieldDescriptor":
        return JoinFieldDescriptor(field_name)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.field_name == other.field_name
            and self.field_data_type == other.field_data_type
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.field_name,
                self.field_data_type,
            )
        )


def temporal_join(
    left: NativeSparkDataFrame,
    right: NativeSparkDataFrame,
    keys: Sequence[str],
) -> NativeSparkDataFrame:
    """
    Temporal join the right dataframe to the left dataframe.

    :param left: The left dataframe.
    :param right: The right dataframe.
    :param keys: The join keys.
    :return: The joined dataframe.
    """

    overlapping_field_names = list(
        set(right.schema.fieldNames()).intersection(set(left.schema.fieldNames()))
    )

    right_aliased = _rename_fields(right, overlapping_field_names)

    equality_predicates = " and ".join([f"{k} = right_{k}" for k in keys])
    time_predicates = (
        f"{EVENT_TIME_ATTRIBUTE_NAME} >= right_{EVENT_TIME_ATTRIBUTE_NAME}"
    )
    join_predicates = " and ".join([equality_predicates, time_predicates])

    partitioned_keys = [f"`{k}`" for k in keys]
    partitioned_keys.append(EVENT_TIME_ATTRIBUTE_NAME)
    window = Window.partitionBy(partitioned_keys).orderBy(
        functions.col(f"right_{EVENT_TIME_ATTRIBUTE_NAME}").desc()
    )

    result_table = (
        left.join(right_aliased, functions.expr(join_predicates), "left_outer")
        .withColumn("row_num", functions.row_number().over(window))
        .filter("row_num == 1")
    )

    return result_table


def _rename_fields(
    df: NativeSparkDataFrame, fields: Sequence[str]
) -> NativeSparkDataFrame:
    right_aliased = df
    for name in (f for f in df.schema.fieldNames() if f in fields):
        right_aliased = right_aliased.withColumnRenamed(name, f"right_{name}")

    return right_aliased

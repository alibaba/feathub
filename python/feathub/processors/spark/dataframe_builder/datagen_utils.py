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
import random
import string
from datetime import datetime, timedelta
from typing import Callable, Any, List, Union

import numpy as np
from pyspark.sql import DataFrame as NativeSparkDataFrame, SparkSession
from pyspark.sql.functions import udf, col

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sources.datagen_source import (
    DataGenSource,
    RandomField,
    SequenceField,
)
from feathub.processors.spark.dataframe_builder.time_utils import (
    append_unix_time_attribute_column,
)
from feathub.processors.spark.spark_types_utils import to_spark_type


def _generate_random_field_name(occupied_field_names: List[str]) -> str:
    prefix = "seed"
    affix = 0
    while True:
        field_name = prefix + str(affix)
        if field_name in occupied_field_names:
            affix += 1
            continue
        return field_name


def _get_udf_mapping(
    field_type: types.DType, field_config: Union[RandomField, SequenceField]
) -> Callable[[int], Any]:
    if isinstance(field_config, SequenceField):
        offset = field_config.start
        if field_type == types.Int64 or field_type == types.Int32:
            return lambda x: x + offset
        elif field_type == types.Float64 or field_type == types.Float32:
            return lambda x: float(x + offset)
        elif field_type == types.String:
            return lambda x: str(x + offset)
        else:
            raise FeathubException(f"Unsupported data type {field_type}")
    elif isinstance(field_config, RandomField):
        if field_type == types.Int64:
            minimum = (
                np.iinfo(np.int64).min
                if field_config.minimum is None
                else field_config.minimum
            )
            maximum = (
                np.iinfo(np.int64).max
                if field_config.maximum is None
                else field_config.maximum
            )
            return lambda seed: random.Random(seed).randint(minimum, maximum)
        elif field_type == types.Int32:
            minimum = (
                np.iinfo(np.int32).min
                if field_config.minimum is None
                else field_config.minimum
            )
            maximum = (
                np.iinfo(np.int32).max
                if field_config.maximum is None
                else field_config.maximum
            )
            return lambda seed: random.Random(seed).randint(minimum, maximum)
        elif field_type == types.Float64:
            minimum = float(
                np.finfo(np.float64).min
                if field_config.minimum is None
                else field_config.minimum
            )
            maximum = float(
                np.finfo(np.float64).max
                if field_config.maximum is None
                else field_config.maximum
            )
            return lambda seed: random.Random(seed).uniform(minimum, maximum)
        elif field_type == types.Float32:
            minimum = float(
                np.finfo(np.float32).min
                if field_config.minimum is None
                else field_config.minimum
            )
            maximum = float(
                np.finfo(np.float32).max
                if field_config.maximum is None
                else field_config.maximum
            )
            return lambda seed: random.Random(seed).uniform(minimum, maximum)
        elif field_type == types.String:
            size = field_config.length
            return lambda seed: "".join(
                random.Random(seed).choices(
                    string.ascii_letters + string.digits, k=size
                )
            )
        elif field_type == types.Bool:
            return lambda seed: bool(random.Random(seed).getrandbits(1))
        elif field_type == types.Timestamp:
            seconds = field_config.max_past / timedelta(seconds=1)
            return lambda seed: datetime.fromtimestamp(
                random.Random(seed).uniform(0, seconds)
            )
        elif isinstance(field_type, types.VectorType):
            element_mapping = _get_udf_mapping(field_type.dtype, field_config)
            size = field_config.length
            return lambda seed: [element_mapping(i) for i in range(size)]
        elif isinstance(field_type, types.MapType):
            key_mapping = _get_udf_mapping(field_type.key_dtype, field_config)
            value_mapping = _get_udf_mapping(field_type.value_dtype, field_config)
            return lambda seed: {key_mapping(seed): value_mapping(seed)}
        else:
            raise FeathubException(f"Unsupported data type {field_type}")


def get_dataframe_from_data_gen_source(
    spark_session: SparkSession, source: DataGenSource
) -> NativeSparkDataFrame:
    if source.number_of_rows is None:
        raise FeathubException(
            "SparkProcessor does not support generating unbounded data."
        )

    seed_field_name = _generate_random_field_name(source.schema.field_names)
    df = spark_session.range(source.number_of_rows).select(
        col("id").alias(seed_field_name)
    )

    for field_name in source.schema.field_names:
        field_type = source.schema.get_field_type(field_name)
        field_config = source.field_configs.get(field_name)

        mapper = _get_udf_mapping(field_type, field_config)

        mapper_udf = udf(mapper, returnType=to_spark_type(field_type))
        df = df.withColumn(field_name, mapper_udf(seed_field_name))

    df = df.drop(seed_field_name)

    df = (
        df
        if source.timestamp_field is None
        else append_unix_time_attribute_column(
            df,
            source.timestamp_field,
            source.timestamp_format,
        )
    )

    return df

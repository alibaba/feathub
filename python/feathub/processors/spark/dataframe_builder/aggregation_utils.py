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
from datetime import timedelta

from feathub.common.exceptions import FeathubException
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.spark.dataframe_builder.spark_sql_expr_utils import (
    to_spark_sql_expr,
)
from feathub.processors.spark.spark_types_utils import to_spark_type
from pyspark.sql.types import DataType


class AggregationFieldDescriptor:
    """
    Descriptor of a field computed by aggregation.
    """

    def __init__(
        self,
        field_name: str,
        field_data_type: DataType,
        expr: str,
        agg_func: AggFunc,
        window_size: timedelta,
    ) -> None:
        self.field_name = field_name
        self.field_data_type = field_data_type
        self.expr = expr
        self.agg_func = agg_func
        self.window_size = window_size

    @staticmethod
    def from_feature(feature: Feature) -> "AggregationFieldDescriptor":
        transform = feature.transform
        if not (
            isinstance(transform, SlidingWindowTransform)
            or isinstance(transform, OverWindowTransform)
        ):
            raise FeathubException(
                f"Cannot convert {feature} to AggregationFieldDescriptor."
            )
        return AggregationFieldDescriptor(
            feature.name,
            to_spark_type(feature.dtype),
            to_spark_sql_expr(transform.expr),
            transform.agg_func,
            transform.window_size,
        )

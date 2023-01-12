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
from typing import Dict, Tuple

from pyspark.sql import DataFrame as NativeSparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr as native_spark_expr

from feathub.common.exceptions import FeathubException
from feathub.common.types import DType
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.processors.spark.dataframe_builder.source_sink_utils import (
    get_dataframe_from_source,
)
from feathub.processors.spark.dataframe_builder.spark_sql_expr_utils import (
    to_spark_sql_expr,
)
from feathub.processors.spark.spark_types_utils import (
    to_spark_type,
)
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class SparkDataFrameBuilder:
    """SparkDataFrameBuilder is used to convert Feathub feature to a Spark DataFrame."""

    def __init__(self, spark_session: SparkSession, registry: Registry):
        """
        Instantiate the SparkDataFrameBuilder.

        :param spark_session: The SparkSession where the DataFrames are created.
        """
        self._spark_session = spark_session
        self._registry = registry

        self._built_dataframes: Dict[
            str, Tuple[TableDescriptor, NativeSparkDataFrame]
        ] = {}

    def build(
        self,
        features: TableDescriptor,
    ) -> NativeSparkDataFrame:
        """
        Convert the given features to native Spark DataFrame.

        If the given features is a FeatureView, it must be resolved, otherwise
        exception will be thrown.

        :param features: The feature to convert to Spark DataFrame.
        :return: The native Spark DataFrame that represents the given features.
        """

        if isinstance(features, FeatureView) and features.is_unresolved():
            raise FeathubException(
                "Trying to convert an unresolved FeatureView to native Spark DataFrame."
            )

        dataframe = self._get_spark_dataframe(features)

        self._built_dataframes.clear()

        return dataframe

    def _get_spark_dataframe(self, features: TableDescriptor) -> NativeSparkDataFrame:
        if features.name in self._built_dataframes:
            if features != self._built_dataframes[features.name][0]:
                raise FeathubException(
                    f"Encounter different TableDescriptor with same name. {features} "
                    f"and {self._built_dataframes[features.name][0]}."
                )
            return self._built_dataframes[features.name][1]

        if isinstance(features, FeatureTable):
            spark_dataframe = get_dataframe_from_source(self._spark_session, features)
        elif isinstance(features, DerivedFeatureView):
            spark_dataframe = self._get_dataframe_from_derived_feature_view(features)
        else:
            raise FeathubException(
                f"Unsupported type '{type(features).__name__}' for '{features}'."
            )

        self._built_dataframes[features.name] = (features, spark_dataframe)

        return spark_dataframe

    def _get_dataframe_from_derived_feature_view(
        self, feature_view: DerivedFeatureView
    ) -> NativeSparkDataFrame:
        source_dataframe = self._get_spark_dataframe(feature_view.get_resolved_source())
        tmp_dataframe = source_dataframe

        dependent_features = []
        for feature in feature_view.get_resolved_features():
            for input_feature in feature.input_features:
                if input_feature not in dependent_features:
                    dependent_features.append(input_feature)
            if feature not in dependent_features:
                dependent_features.append(feature)

        for feature in dependent_features:
            if isinstance(feature.transform, ExpressionTransform):
                tmp_dataframe = self._evaluate_expression_transform(
                    tmp_dataframe,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            else:
                raise RuntimeError(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        output_fields = feature_view.get_output_fields(
            source_fields=source_dataframe.schema.fieldNames()
        )
        return tmp_dataframe.select(output_fields)

    @staticmethod
    def _evaluate_expression_transform(
        source_dataframe: NativeSparkDataFrame,
        transform: ExpressionTransform,
        result_field_name: str,
        result_type: DType,
    ) -> NativeSparkDataFrame:
        spark_sql_expr = to_spark_sql_expr(transform.expr)
        result_spark_type = to_spark_type(result_type)

        return source_dataframe.withColumn(
            result_field_name, native_spark_expr(spark_sql_expr).cast(result_spark_type)
        )

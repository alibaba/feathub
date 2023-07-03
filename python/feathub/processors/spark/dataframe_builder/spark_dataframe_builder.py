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
from datetime import datetime
from typing import Dict, Tuple, List, Sequence, Union, Optional

import pandas as pd

from feathub.dsl.expr_utils import is_id, get_var_name
from feathub.feature_views.transforms.join_transform import JoinTransform
from pyspark.sql import DataFrame as NativeSparkDataFrame, functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, struct

from feathub.common.exceptions import (
    FeathubException,
    FeathubTransformationException,
)
from feathub.common.types import DType
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.spark.dataframe_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)
from feathub.processors.spark.dataframe_builder.over_window_utils import (
    OverWindowDescriptor,
    evaluate_over_window_transform,
)
from feathub.processors.spark.dataframe_builder.source_sink_utils import (
    get_dataframe_from_source,
)
from feathub.processors.spark.dataframe_builder.spark_sql_expr_utils import (
    to_spark_sql_expr,
)
from feathub.processors.spark.dataframe_builder.join_utils import (
    JoinFieldDescriptor,
    temporal_join,
)
from feathub.processors.spark.spark_types_utils import (
    to_spark_type,
)
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class SparkDataFrameBuilder:
    """SparkDataFrameBuilder is used to convert FeatHub feature to a Spark DataFrame."""

    def __init__(self, spark_session: SparkSession, registry: Registry):
        """
        Instantiate the SparkDataFrameBuilder.

        :param spark_session: The SparkSession where the DataFrames are created.
        :param registry: The FeatHub registry.
        """
        self._spark_session = spark_session
        self._registry = registry

        self._built_dataframes: Dict[
            str, Tuple[TableDescriptor, NativeSparkDataFrame]
        ] = {}

    def build(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> NativeSparkDataFrame:
        """
        Convert the given features to native Spark DataFrame.

        If the given features is a FeatureView, it must be resolved, otherwise
        exception will be thrown.

        :param features: The feature to convert to Spark DataFrame.
        :param keys: Optional. If it is not none, then the returned table only includes
                     rows whose key fields match at least one row of the keys.
        :param start_datetime: Optional. If it is not None, the `features` table should
                               have a timestamp field. And the output table will only
                               include features whose
                               timestamp >= start_datetime. If any field (e.g. minute)
                               is not specified in the start_datetime, we assume this
                               field has the minimum possible value.
        :param end_datetime: Optional. If it is not None, the `features` table should
                             have a timestamp field. And the output table will only
                             include features whose timestamp < end_datetime. If any
                             field (e.g. minute) is not specified in the end_datetime,
                             we assume this field has the maximum possible value.
        :return: The native Spark DataFrame that represents the given features.
        """

        if isinstance(features, FeatureView) and features.is_unresolved():
            raise FeathubException(
                "Trying to convert an unresolved FeatureView to native Spark DataFrame."
            )

        dataframe = self._get_spark_dataframe(features)

        if keys is not None:
            dataframe = self._filter_dataframe_by_keys(dataframe, keys)

        if start_datetime is not None or end_datetime is not None:
            if features.timestamp_field is None:
                raise FeathubException(
                    "Feature is missing timestamp_field. It cannot be ranged "
                    "by start_datetime or end_datetime."
                )
            dataframe = self._filter_dataframe_by_time(
                dataframe, start_datetime, end_datetime
            )

        if EVENT_TIME_ATTRIBUTE_NAME in dataframe.columns:
            dataframe = dataframe.drop(EVENT_TIME_ATTRIBUTE_NAME)

        self._built_dataframes.clear()

        return dataframe

    def _filter_dataframe_by_keys(
        self,
        df: NativeSparkDataFrame,
        keys: Union[pd.DataFrame, TableDescriptor],
    ) -> NativeSparkDataFrame:
        key_df = self._get_spark_dataframe(keys)
        for field_name in key_df.schema.fieldNames():
            if field_name not in df.schema.fieldNames():
                raise FeathubException(
                    f"Given key {field_name} not in the fields: "
                    f"{df.schema.fieldNames()}."
                )
        return df.join(key_df, key_df.schema.fieldNames())

    def _get_spark_dataframe(
        self, features: Union[TableDescriptor, pd.DataFrame]
    ) -> NativeSparkDataFrame:
        if isinstance(features, pd.DataFrame):
            return self._spark_session.createDataFrame(features)

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
        window_agg_map: Dict[
            OverWindowDescriptor, List[AggregationFieldDescriptor]
        ] = {}

        dataframe_by_names = {}
        descriptors_by_names = {}

        table_names = set(
            [
                feature.transform.table_name
                for feature in feature_view.get_resolved_features()
                if isinstance(feature.transform, JoinTransform)
            ]
        )
        for name in table_names:
            descriptor = self._registry.get_features(name=name)
            descriptors_by_names[name] = descriptor
            dataframe_by_names[name] = self._get_spark_dataframe(features=descriptor)

        for feature in feature_view.get_resolved_features():
            for input_feature in feature.input_features:
                if input_feature not in dependent_features:
                    dependent_features.append(input_feature)
            if feature not in dependent_features:
                dependent_features.append(feature)

        # The right_tables map keeps track of the information of the right table to join
        # with the source table. The key is a tuple of right_table_name and join_keys
        # and the value is a map from the name of the field of the right table
        # to join to JoinFieldDescriptor.
        right_tables: Dict[
            Tuple[str, Sequence[str]], Dict[str, JoinFieldDescriptor]
        ] = {}

        # This list contains all per-row transform features listed after the first
        # JoinTransform or OverWindowTransform feature in the dependent_features.
        # These features are evaluated after all over windows and table join.
        per_row_transform_features_following_first_over_window_or_join = []

        for feature in dependent_features:
            if isinstance(feature.transform, ExpressionTransform):
                if len(right_tables) > 0 or len(window_agg_map) > 0:
                    per_row_transform_features_following_first_over_window_or_join.append(  # noqa
                        feature
                    )
                else:
                    tmp_dataframe = self._evaluate_expression_transform(
                        tmp_dataframe,
                        feature.transform,
                        feature.name,
                        feature.dtype,
                    )
            elif isinstance(feature.transform, PythonUdfTransform):
                if len(right_tables) > 0 or len(window_agg_map) > 0:
                    per_row_transform_features_following_first_over_window_or_join.append(  # noqa
                        feature
                    )
                else:
                    tmp_dataframe = self._evaluate_python_udf_transform(
                        tmp_dataframe, feature.transform, feature.name, feature.dtype
                    )
            elif isinstance(feature.transform, OverWindowTransform):
                transform = feature.transform

                if transform.window_size is not None or transform.limit is not None:
                    if transform.agg_func == AggFunc.ROW_NUMBER:
                        raise FeathubTransformationException(
                            "ROW_NUMBER can only work without window_size and limit."
                        )

                window_aggs = window_agg_map.setdefault(
                    OverWindowDescriptor.from_over_window_transform(transform),
                    [],
                )
                window_aggs.append(AggregationFieldDescriptor.from_feature(feature))
            elif isinstance(feature.transform, JoinTransform):
                if feature.keys is None:
                    raise FeathubException(
                        f"SparkProcessor cannot join feature {feature} without key."
                    )
                if not all(
                    key in source_dataframe.schema.fieldNames() for key in feature.keys
                ):
                    raise FeathubException(
                        f"Source dataframe {source_dataframe.schema.fieldNames()} "
                        f"does not have the keys of the Feature to join {feature.keys}."
                    )

                join_transform = feature.transform
                if not is_id(join_transform.expr):
                    raise FeathubException(
                        "It is not supported to use Feathub expression in JoinTransform"
                        " for spark processor."
                    )
                right_table_descriptor = descriptors_by_names[join_transform.table_name]
                if right_table_descriptor.timestamp_field is None:
                    raise FeathubException(
                        f"SparkProcessor cannot join with {right_table_descriptor} "
                        f"without timestamp field."
                    )
                join_field_descriptors = right_tables.setdefault(
                    (join_transform.table_name, tuple(feature.keys)), dict()
                )
                join_field_descriptors.update(
                    {
                        key: JoinFieldDescriptor.from_field_name(key)
                        for key in feature.keys
                    }
                )
                join_field_descriptors[
                    EVENT_TIME_ATTRIBUTE_NAME
                ] = JoinFieldDescriptor.from_field_name(EVENT_TIME_ATTRIBUTE_NAME)

                join_field_descriptors[
                    join_transform.expr
                ] = JoinFieldDescriptor.from_table_descriptor_and_field_name(
                    right_table_descriptor, get_var_name(join_transform.expr)
                )
            else:
                raise RuntimeError(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        for (
            right_table_name,
            keys,
        ), join_field_descriptors in right_tables.items():
            right_dataframe = dataframe_by_names[right_table_name].select(
                *[
                    functions.col(right_table_field)
                    for right_table_field in join_field_descriptors.keys()
                ]
            )
            tmp_dataframe = temporal_join(
                tmp_dataframe,
                right_dataframe,
                keys,
            )

        for over_window_descriptor, agg_descriptor in window_agg_map.items():
            tmp_dataframe = evaluate_over_window_transform(
                tmp_dataframe,
                over_window_descriptor,
                agg_descriptor,
            )

        for feature in per_row_transform_features_following_first_over_window_or_join:
            if isinstance(feature.transform, ExpressionTransform):
                tmp_dataframe = self._evaluate_expression_transform(
                    tmp_dataframe,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            elif isinstance(feature.transform, PythonUdfTransform):
                tmp_dataframe = self._evaluate_python_udf_transform(
                    tmp_dataframe, feature.transform, feature.name, feature.dtype
                )
            else:
                raise RuntimeError(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        # TODO: Apply filtering as early as possible to improve performance.
        if feature_view.filter_expr is not None:
            tmp_dataframe = tmp_dataframe.filter(
                functions.expr(to_spark_sql_expr(feature_view.filter_expr))
            )

        output_fields = feature_view.get_output_fields(
            source_fields=source_dataframe.schema.fieldNames()
        )
        return tmp_dataframe.select(output_fields)

    @staticmethod
    def _filter_dataframe_by_time(
        dataframe: NativeSparkDataFrame,
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
    ) -> NativeSparkDataFrame:
        if start_datetime is not None:
            dataframe = dataframe.filter(
                functions.expr(
                    EVENT_TIME_ATTRIBUTE_NAME
                    + " >= "
                    + str(int(start_datetime.timestamp() * 1000))
                )
            )
        if end_datetime is not None:
            dataframe = dataframe.filter(
                functions.expr(
                    EVENT_TIME_ATTRIBUTE_NAME
                    + " < "
                    + str(int(end_datetime.timestamp() * 1000))
                )
            )
        return dataframe

    @staticmethod
    def _evaluate_python_udf_transform(
        source_dataframe: NativeSparkDataFrame,
        transform: PythonUdfTransform,
        result_field_name: str,
        result_type: DType,
    ) -> NativeSparkDataFrame:
        python_udf = udf(transform.udf, returnType=to_spark_type(result_type))
        return source_dataframe.withColumn(
            result_field_name,
            python_udf(struct([source_dataframe[x] for x in source_dataframe.columns])),
        )

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
            result_field_name, functions.expr(spark_sql_expr).cast(result_spark_type)
        )

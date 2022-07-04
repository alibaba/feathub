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
from datetime import datetime, timedelta
from typing import Union, Optional, List, Any, Dict, Sequence

import pandas as pd
from feathub.common import types
from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    TableDescriptor as NativeFlinkTableDescriptor,
    Schema as NativeFlinkSchema,
)
from pyflink.table.window import Over, OverWindowPartitionedOrderedPreceding

from feathub.common.exceptions import FeathubException, FeathubTransformationException
from feathub.common.types import DType
from feathub.common.utils import to_java_date_format
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.joined_feature_view import JoinedFeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.window_agg_transform import WindowAggTransform
from feathub.processors.flink.flink_types_utils import to_flink_schema, to_flink_type
from feathub.registries.registry import Registry
from feathub.sources.file_source import FileSource
from feathub.table.table_descriptor import TableDescriptor


class FlinkTableBuilder:
    """FlinkTableBuilder is used to convert Feathub feature to a Flink Table."""

    _EVENT_TIME_ATTRIBUTE_NAME = "__event_time_attribute__"

    def __init__(
        self,
        t_env: StreamTableEnvironment,
        registry: Registry,
        processor_config: Dict[str, Any],
    ):
        """
        Instantiate the FlinkTableBuilder.

        :param t_env: The Flink StreamTableEnvironment under which the Tables to be
                      created.
        :param registry: The Feathub registry.
        :param processor_config: The Flink processor configuration.
        """
        self.t_env = t_env
        self.registry = registry
        self.max_out_of_orderness_interval = processor_config.get(
            "max_out_of_orderness_interval", "INTERVAL '60' SECOND"
        )

    def build(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> NativeFlinkTable:
        """
        Convert the given features to native Flink table.

        If the given features is a FeatureView, it must be resolved, otherwise
        exception will be thrown.

        :param features: The feature to converts to native Flink table.
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
        :return: The native Flink table that represents the given features.
        """
        if isinstance(features, FeatureView) and features.is_unresolved():
            raise FeathubException(
                "Trying to convert a unresolved FeatureView to native Flink table."
            )

        table = self._get_table(features)

        if keys is not None:
            table = self._filter_table_by_keys(table, keys)

        if start_datetime is not None or end_datetime is not None:
            if features.timestamp_field is None:
                raise FeathubException(
                    "Feature is missing timestamp_field. It cannot be ranged "
                    "by start_datetime."
                )
            table = self._range_table_by_time(table, start_datetime, end_datetime)

        if (
            FlinkTableBuilder._EVENT_TIME_ATTRIBUTE_NAME
            in table.get_schema().get_field_names()
        ):
            table = table.drop_columns(FlinkTableBuilder._EVENT_TIME_ATTRIBUTE_NAME)

        return table

    def _filter_table_by_keys(
        self,
        table: NativeFlinkTable,
        keys: Union[pd.DataFrame, TableDescriptor],
    ) -> NativeFlinkTable:
        if keys is not None:
            key_table = self._get_table(keys)
            for field_name in key_table.get_schema().get_field_names():
                if field_name not in table.get_schema().get_field_names():
                    raise FeathubException(
                        f"Given key {field_name} not in the table fields "
                        f"{table.get_schema().get_field_names()}."
                    )
            table = self._join_table_on_key(
                key_table, table, key_table.get_schema().get_field_names()
            )
        return table

    def _range_table_by_time(
        self,
        table: NativeFlinkTable,
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
    ):
        if start_datetime is not None:
            table = table.filter(
                native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME).__ge__(
                    native_flink_expr.lit(
                        start_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    ).to_timestamp
                )
            )
        if end_datetime is not None:
            table = table.filter(
                native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME).__lt__(
                    native_flink_expr.lit(
                        end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    ).to_timestamp
                )
            )
        return table

    def _get_table(
        self, features: Union[TableDescriptor, pd.DataFrame]
    ) -> NativeFlinkTable:
        if isinstance(features, pd.DataFrame):
            return self.t_env.from_pandas(features)
        elif isinstance(features, FileSource):
            return self._get_table_from_file_source(features)
        elif isinstance(features, DerivedFeatureView):
            return self._get_table_from_derived_feature_view(features)
        elif isinstance(features, JoinedFeatureView):
            return self._get_table_from_joined_feature_view(features)

        raise FeathubException(
            f"Unsupported type '{type(features).__name__}' for '{features}'."
        )

    def _get_table_from_file_source(self, source: FileSource) -> NativeFlinkTable:
        schema = source.schema
        if schema is None:
            raise FeathubException(
                "Flink processor requires schema for the FileSource."
            )

        flink_schema = to_flink_schema(schema)

        # Define watermark if the source has timestamp field
        if source.timestamp_field is not None:
            flink_schema = self._define_watermark(
                flink_schema,
                source.timestamp_field,
                source.timestamp_format,
                schema.get_field_type(source.timestamp_field),
            )

        descriptor_builder = (
            NativeFlinkTableDescriptor.for_connector("filesystem")
            .format(source.file_format)
            .option("path", source.path)
            .schema(flink_schema)
        )

        if source.file_format == "csv":
            # Set ignore-parse-errors to set null in case of csv parse error
            descriptor_builder.option("csv.ignore-parse-errors", "true")

        table = self.t_env.from_descriptor(descriptor_builder.build())
        return table

    def _define_watermark(
        self,
        flink_schema: NativeFlinkSchema,
        timestamp_field: str,
        timestamp_format: str,
        timestamp_field_dtype: DType,
    ) -> NativeFlinkSchema:
        builder = NativeFlinkSchema.new_builder()
        builder.from_schema(flink_schema)

        if timestamp_format == "epoch":
            if (
                timestamp_field_dtype != types.Int32
                or timestamp_field_dtype != types.Int64
            ):
                raise FeathubException(
                    "Timestamp field with epoch format only supports data type of "
                    "Int32 and Int64."
                )
            builder.column_by_expression(
                self._EVENT_TIME_ATTRIBUTE_NAME,
                f"CAST("
                f"  FROM_UNIXTIME(CAST(`{timestamp_field}` AS INTEGER)) "
                f"AS TIMESTAMP(3))",
            )
        else:
            if timestamp_field_dtype != types.String:
                raise FeathubException(
                    "Timestamp field with non epoch format only "
                    "supports data type of String."
                )
            java_datetime_format = to_java_date_format(timestamp_format).replace(
                "'", "''"  # Escape single quote for sql
            )
            builder.column_by_expression(
                self._EVENT_TIME_ATTRIBUTE_NAME,
                f"TO_TIMESTAMP(`{timestamp_field}`, '{java_datetime_format}')",
            )

        builder.watermark(
            self._EVENT_TIME_ATTRIBUTE_NAME,
            watermark_expr=f"`{self._EVENT_TIME_ATTRIBUTE_NAME}` "
            f"- {self.max_out_of_orderness_interval}",
        )
        return builder.build()

    def _get_table_from_derived_feature_view(
        self, feature_view: DerivedFeatureView
    ) -> NativeFlinkTable:
        source_table = self._get_table(feature_view.source)
        source_fields = list(source_table.get_schema().get_field_names())
        dependent_features = []

        for feature in feature_view.get_resolved_features():
            for input_feature in feature.input_features:
                if input_feature not in dependent_features:
                    dependent_features.append(input_feature)
            if feature not in dependent_features:
                dependent_features.append(feature)

        tmp_table = source_table
        for feature in dependent_features:
            if feature.name in tmp_table.get_schema().get_field_names():
                continue
            if isinstance(feature.transform, ExpressionTransform):
                tmp_table = self._evaluate_expression_transform(
                    tmp_table,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            elif isinstance(feature.transform, WindowAggTransform):
                # We can optimize by merging feature with the same window size
                if feature_view.timestamp_field is None:
                    raise FeathubException(
                        "FeatureView must have timestamp field for WindowAggTransform."
                    )
                tmp_table = self._evaluate_window_transform(
                    tmp_table,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )
        output_fields = self._get_output_fields(feature_view, source_fields)
        return tmp_table.select(
            *[native_flink_expr.col(field) for field in output_fields]
        )

    def _get_table_from_joined_feature_view(
        self,
        feature_view: JoinedFeatureView,
    ) -> NativeFlinkTable:
        if feature_view.timestamp_field is None:
            raise FeathubException(
                f"FlinkProcessor cannot process JoinedFeatureView {feature_view} "
                f"without timestamp field."
            )
        source_table = self._get_table(feature_view.source)
        source_fields = source_table.get_schema().get_field_names()

        table_names = set(
            [
                feature.transform.table_name
                for feature in feature_view.get_resolved_features()
                if isinstance(feature.transform, JoinTransform)
            ]
        )

        table_by_names = {}
        descriptors_by_names = {}
        for name in table_names:
            descriptor = self.registry.get_features(name=name)
            descriptors_by_names[name] = descriptor
            table_by_names[name] = self._get_table(features=descriptor)

        tmp_table = source_table
        for feature in feature_view.get_resolved_features():
            if feature.name in tmp_table.get_schema().get_field_names():
                continue

            if isinstance(feature.transform, JoinTransform):
                if feature.keys is None:
                    raise FeathubException(
                        f"FlinkProcessor cannot join feature {feature} without key."
                    )
                join_transform = feature.transform
                right_table_descriptor = descriptors_by_names[join_transform.table_name]
                right_timestamp_field = right_table_descriptor.timestamp_field
                if right_timestamp_field is None:
                    raise FeathubException(
                        f"FlinkProcessor cannot join with {right_table_descriptor} "
                        f"without timestamp field."
                    )
                right_table_fields = [
                    native_flink_expr.col(key) for key in feature.keys
                ] + [
                    native_flink_expr.col(feature.name),
                    native_flink_expr.col(right_timestamp_field),
                    native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME),
                ]
                right_table = table_by_names[join_transform.table_name].select(
                    *right_table_fields
                )
                tmp_table = self._temporal_join(
                    tmp_table,
                    right_table,
                    feature.keys,
                )
            elif isinstance(feature.transform, ExpressionTransform):
                tmp_table = self._evaluate_expression_transform(
                    tmp_table, feature.transform, feature.name, feature.dtype
                )
            else:
                raise RuntimeError(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        output_fields = self._get_output_fields(feature_view, source_fields)
        return tmp_table.select(
            *[native_flink_expr.col(field) for field in output_fields]
        )

    def _get_output_fields(
        self, feature_view: FeatureView, source_fields: List[str]
    ) -> List[str]:
        output_fields = feature_view.get_output_fields(source_fields)
        if self._EVENT_TIME_ATTRIBUTE_NAME not in output_fields:
            output_fields.append(self._EVENT_TIME_ATTRIBUTE_NAME)
        return output_fields

    def _join_table_on_key(
        self, left: NativeFlinkTable, right: NativeFlinkTable, key_fields: List[str]
    ) -> NativeFlinkTable:
        aliased_right_table_field_names = [
            f"right.{f}" if f in key_fields else f
            for f in right.get_schema().get_field_names()
        ]
        right_aliased = right.alias(*aliased_right_table_field_names)

        predicates = [
            left.__getattr__(f"{key}") == right_aliased.__getattr__(f"right.{key}")
            for key in key_fields
        ]

        if len(predicates) > 1:
            predicate = native_flink_expr.and_(*predicates)
        else:
            predicate = predicates[0]

        joined_table = left.join(right_aliased, predicate).select(
            *[
                native_flink_expr.col(left_field_name)
                for left_field_name in left.get_schema().get_field_names()
            ],
            *[
                native_flink_expr.col(right_field_name)
                for right_field_name in right.get_schema().get_field_names()
                if right_field_name not in key_fields
            ],
        )

        return joined_table

    def _temporal_join(
        self,
        left: NativeFlinkTable,
        right: NativeFlinkTable,
        keys: Sequence[str],
    ) -> NativeFlinkTable:
        self.t_env.create_temporary_view("left_table", left)
        self.t_env.create_temporary_view("right_table", right)
        escaped_keys = [f"`{k}`" for k in keys]
        temporal_right_table = self.t_env.sql_query(
            f"""
        SELECT * FROM (SELECT *,
            ROW_NUMBER() OVER (PARTITION BY {",".join(escaped_keys)}
                ORDER BY `{self._EVENT_TIME_ATTRIBUTE_NAME}` DESC) AS rownum
            FROM right_table)
        WHERE rownum = 1
        """
        )
        self.t_env.create_temporary_view("temporal_right_table", temporal_right_table)

        predicates = " and ".join(
            [f"left_table.{k} = temporal_right_table.{k}" for k in escaped_keys]
        )

        temporal_join_query = f"""
        SELECT * FROM left_table LEFT JOIN
            temporal_right_table
            FOR SYSTEM_TIME AS OF left_table.`{self._EVENT_TIME_ATTRIBUTE_NAME}`
        ON {predicates}
        """

        result_table = self.t_env.sql_query(temporal_join_query)

        self.t_env.drop_temporary_view("left_table")
        self.t_env.drop_temporary_view("right_table")
        self.t_env.drop_temporary_view("temporal_right_table")
        return result_table

    def _evaluate_expression_transform(
        self,
        source_table: NativeFlinkTable,
        transform: ExpressionTransform,
        result_field_name: str,
        result_type: DType,
    ) -> NativeFlinkTable:
        result_type = to_flink_type(result_type)
        return source_table.add_or_replace_columns(
            native_flink_expr.call_sql(transform.expr)
            .cast(result_type)
            .alias(result_field_name)
        )

    def _evaluate_window_transform(
        self,
        flink_table: NativeFlinkTable,
        transform: WindowAggTransform,
        result_field_name: str,
        result_type: DType,
    ) -> NativeFlinkTable:
        window = self._get_flink_window(transform)

        result_type = to_flink_type(result_type)
        result_table = flink_table.over_window(window.alias("w")).select(
            *[
                native_flink_expr.col(field_name)
                for field_name in flink_table.get_schema().get_field_names()
            ],
            self._get_agg_select_expr(
                native_flink_expr.call_sql(transform.expr).cast(result_type),
                transform,
                "w",
            ).alias(result_field_name),
        )

        return result_table

    def _get_agg_select_expr(
        self,
        col: native_flink_expr.Expression,
        transformation: WindowAggTransform,
        window_alias: str,
    ) -> native_flink_expr.Expression:
        agg_func = transformation.agg_func
        if agg_func == "AVG":
            result = col.avg
        elif agg_func == "MIN":
            result = col.min
        elif agg_func == "MAX":
            result = col.max
        elif agg_func == "SUM":
            result = col.sum
        else:
            raise FeathubTransformationException(
                f"Unsupported aggregation for FlinkProcessor {agg_func}."
            )
        return result.over(native_flink_expr.col(window_alias))

    def _get_flink_window(
        self, transform: WindowAggTransform
    ) -> OverWindowPartitionedOrderedPreceding:
        if transform.window_size is None:
            raise FeathubTransformationException(
                "Window Aggregation without window size is not supported "
                "in FlinkProcessor."
            )
        if transform.limit is not None:
            raise FeathubTransformationException(
                "Window Aggregation with limit is not supported in FlinkProcessor."
            )

        # Group by key
        if len(transform.group_by_keys) == 0:
            window = Over.order_by(
                native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME)
            )
        else:
            keys = [native_flink_expr.col(key) for key in transform.group_by_keys]
            window = Over.partition_by(*keys).order_by(
                native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME)
            )

        return window.preceding(
            native_flink_expr.lit(
                transform.window_size / timedelta(milliseconds=1)
            ).milli
        )

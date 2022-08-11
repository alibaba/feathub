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
from typing import Union, Optional, List, Any, Dict, Sequence, Callable, Set, Tuple

import pandas as pd
from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    TableDescriptor as NativeFlinkTableDescriptor,
    Schema as NativeFlinkSchema,
    AggregateFunction,
)
from pyflink.table.types import DataType
from pyflink.table.udf import ACC, T, udaf
from pyflink.table.window import Over, OverWindowPartitionedOrderedPreceding

from feathub.common import types
from feathub.common.exceptions import FeathubException, FeathubTransformationException
from feathub.common.types import DType
from feathub.common.utils import to_java_date_format
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.joined_feature_view import JoinedFeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.window_agg_transform import (
    WindowAggTransform,
    AggFunc,
)
from feathub.processors.flink.flink_types_utils import to_flink_schema, to_flink_type
from feathub.registries.registry import Registry
from feathub.sources.file_source import FileSource
from feathub.table.table_descriptor import TableDescriptor


def _avg(s: pd.Series) -> Any:
    return s.mean()


def _min(s: pd.Series) -> Any:
    return s.min()


def _max(s: pd.Series) -> Any:
    return s.max()


def _sum(s: pd.Series) -> Any:
    return s.sum()


def _first_value(s: pd.Series) -> Any:
    return s.iloc[0]


def _last_value(s: pd.Series) -> Any:
    return s.iloc[-1]


def _row_num(s: pd.Series) -> Any:
    return s.size


_AGG_FUNCTIONS: Dict[AggFunc, Callable[[pd.Series], Any]] = {
    AggFunc.AVG: _avg,
    AggFunc.SUM: _sum,
    AggFunc.MAX: _max,
    AggFunc.MIN: _min,
    AggFunc.FIRST_VALUE: _first_value,
    AggFunc.LAST_VALUE: _last_value,
    AggFunc.ROW_NUMBER: _row_num,
}


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

        # Mapping from the name of TableDescriptor to the TableDescriptor and the built
        # NativeFlinkTable. This is used as a cache to avoid re-computing the native
        # flink table from the same TableDescriptor.
        self._built_tables: Dict[str, Tuple[TableDescriptor, NativeFlinkTable]] = {}

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
    ) -> NativeFlinkTable:
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

        if features.name in self._built_tables:
            if features != self._built_tables[features.name][0]:
                raise FeathubException(
                    f"Encounter different TableDescriptor with same name. {features} "
                    f"and {self._built_tables[features.name][0]}."
                )
            return self._built_tables[features.name][1]

        if isinstance(features, FileSource):
            self._built_tables[features.name] = (
                features,
                self._get_table_from_file_source(features),
            )
        elif isinstance(features, DerivedFeatureView):
            self._built_tables[features.name] = (
                features,
                self._get_table_from_derived_feature_view(features),
            )
        elif isinstance(features, JoinedFeatureView):
            self._built_tables[features.name] = (
                features,
                self._get_table_from_joined_feature_view(features),
            )
        else:
            raise FeathubException(
                f"Unsupported type '{type(features).__name__}' for '{features}'."
            )

        return self._built_tables[features.name][1]

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
                and timestamp_field_dtype != types.Int64
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

        window_agg_map: Dict[_OverWindowDescriptor, List[_AggregationDescriptor]] = {}

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
                if feature_view.timestamp_field is None:
                    raise FeathubException(
                        "FeatureView must have timestamp field for WindowAggTransform."
                    )
                transform = feature.transform
                window_aggs = window_agg_map.setdefault(
                    _OverWindowDescriptor.from_window_agg_transform(transform),
                    [],
                )
                window_aggs.append(
                    _AggregationDescriptor(
                        feature.name,
                        to_flink_type(feature.dtype),
                        transform.expr,
                        transform.agg_func,
                    )
                )
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        for over_window_descriptor, agg_descriptor in window_agg_map.items():
            tmp_table = self._evaluate_window_transform(
                tmp_table, feature_view.keys, over_window_descriptor, agg_descriptor
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

        # The right_tables map keeps track of the information of the right table to join
        # with the source table. The key is a tuple of right_table_name and join_keys
        # and the value is the names of the field of the right table to join.
        right_tables: Dict[Tuple[str, Sequence[str]], Set[str]] = {}
        tmp_table = source_table
        for feature in feature_view.get_resolved_features():
            if feature.name in tmp_table.get_schema().get_field_names():
                continue

            if isinstance(feature.transform, JoinTransform):
                if feature.keys is None:
                    raise FeathubException(
                        f"FlinkProcessor cannot join feature {feature} without key."
                    )
                if not all(
                    key in source_table.get_schema().get_field_names()
                    for key in feature.keys
                ):
                    raise FeathubException(
                        f"Source table {source_table.get_schema().get_field_names()} "
                        f"doesn't have the keys of the Feature to join {feature.keys}."
                    )

                join_transform = feature.transform
                right_table_descriptor = descriptors_by_names[join_transform.table_name]
                right_timestamp_field = right_table_descriptor.timestamp_field
                if right_timestamp_field is None:
                    raise FeathubException(
                        f"FlinkProcessor cannot join with {right_table_descriptor} "
                        f"without timestamp field."
                    )
                right_table_fields = right_tables.setdefault(
                    (join_transform.table_name, tuple(feature.keys)), set()
                )
                right_table_fields.update(feature.keys)
                right_table_fields.add(feature.name)
                right_table_fields.add(right_timestamp_field)
                right_table_fields.add(self._EVENT_TIME_ATTRIBUTE_NAME)
            elif isinstance(feature.transform, ExpressionTransform):
                tmp_table = self._evaluate_expression_transform(
                    tmp_table, feature.transform, feature.name, feature.dtype
                )
            else:
                raise RuntimeError(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        for (right_table_name, keys), right_table_fields in right_tables.items():
            right_table = table_by_names[right_table_name].select(
                *[
                    native_flink_expr.col(right_table_field)
                    for right_table_field in right_table_fields
                ]
            )
            tmp_table = self._temporal_join(
                tmp_table,
                right_table,
                keys,
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
        keys: List[str],
        window_descriptor: "_OverWindowDescriptor",
        agg_descriptors: List["_AggregationDescriptor"],
    ) -> NativeFlinkTable:
        window = self._get_flink_over_window(window_descriptor)
        if window_descriptor.filter_expr is not None:
            agg_table = (
                flink_table.filter(
                    native_flink_expr.call_sql(window_descriptor.filter_expr)
                )
                .over_window(window.alias("w"))
                .select(
                    *[native_flink_expr.col(key) for key in keys],
                    *self._get_agg_column_list(
                        window_descriptor,
                        agg_descriptors,
                    ),
                    native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME),
                )
            )
            return self._temporal_join(flink_table, agg_table, keys)

        return flink_table.over_window(window.alias("w")).select(
            native_flink_expr.col("*"),
            *self._get_agg_column_list(window_descriptor, agg_descriptors),
        )

    def _get_agg_column_list(
        self,
        window_descriptor: "_OverWindowDescriptor",
        agg_descriptors: List["_AggregationDescriptor"],
    ) -> List[native_flink_expr.Expression]:
        return [
            self._get_agg_select_expr(
                native_flink_expr.call_sql(descriptor.expr).cast(
                    descriptor.result_type
                ),
                descriptor.result_type,
                window_descriptor,
                descriptor.agg_func,
                "w",
            ).alias(descriptor.result_field_name)
            for descriptor in agg_descriptors
        ]

    def _get_agg_select_expr(
        self,
        expr: native_flink_expr.Expression,
        result_type: DataType,
        over_window_descriptor: "_OverWindowDescriptor",
        agg_func: AggFunc,
        window_alias: str,
    ) -> native_flink_expr.Expression:

        if (
            over_window_descriptor.limit is not None
            and over_window_descriptor.window_size is not None
        ):
            # We use PyFlink UDAF to support over window ranged by both time interval
            # and row count.
            if agg_func not in _AGG_FUNCTIONS:
                raise FeathubTransformationException(
                    f"Unsupported aggregation for FlinkProcessor {agg_func}."
                )
            time_windowed_agg_func = _TimeWindowedAggFunction(
                over_window_descriptor.window_size,
                _AGG_FUNCTIONS[agg_func],
            )
            result = native_flink_expr.call(
                udaf(
                    time_windowed_agg_func, result_type=result_type, func_type="pandas"
                ),
                expr,
                native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME),
            )
        else:
            if agg_func == AggFunc.AVG:
                result = expr.avg
            elif agg_func == AggFunc.MIN:
                result = expr.min
            elif agg_func == AggFunc.MAX:
                result = expr.max
            elif agg_func == AggFunc.SUM:
                result = expr.sum
            # TODO: FIRST_VALUE AND LAST_VALUE is supported after PyFlink 1.16 without
            # PyFlink UDAF.
            else:
                raise FeathubTransformationException(
                    f"Unsupported aggregation for FlinkProcessor {agg_func}."
                )
        return result.over(native_flink_expr.col(window_alias))

    def _get_flink_over_window(
        self, over_window_descriptor: "_OverWindowDescriptor"
    ) -> OverWindowPartitionedOrderedPreceding:

        # Group by key
        if len(over_window_descriptor.group_by_keys) == 0:
            window = Over.order_by(
                native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME)
            )
        else:
            keys = [
                native_flink_expr.col(key)
                for key in over_window_descriptor.group_by_keys
            ]
            window = Over.partition_by(*keys).order_by(
                native_flink_expr.col(self._EVENT_TIME_ATTRIBUTE_NAME)
            )

        if over_window_descriptor.limit is not None:
            # Flink over window only support ranging by either row-count or time. For
            # feature that need to range by both row-count and time, it is handled in
            # _get_agg_select_expr with the _TimeWindowedAggFunction UDTAF.
            return window.preceding(
                native_flink_expr.row_interval(over_window_descriptor.limit - 1)
            )

        if over_window_descriptor.window_size is not None:
            return window.preceding(
                native_flink_expr.lit(
                    over_window_descriptor.window_size / timedelta(milliseconds=1)
                ).milli
            )

        return window.preceding(native_flink_expr.UNBOUNDED_RANGE)


# TODO: We can implement the TimeWindowedAggFunction in Java to get better performance.
class _TimeWindowedAggFunction(AggregateFunction):
    """
    An aggregate function for Flink table aggregation.

    The aggregate function only aggregate rows with row time in the range of
    [current_row_time - time_interval, current_row_time]. Currently, Flink SQL/Table
    only support over window with either time range or row count-based range. This can
    be used with a row count-based over window to achieve over window with both time
    range and row count-based range.
    """

    def __init__(self, time_interval: timedelta, agg_func: Callable[[pd.Series], Any]):
        """
        Instantiate a _TimeWindowedAggFunction.

        :param time_interval: Only rows with row time in the range of [current_row_time
                              - time_interval, current_row_time] is included in the
                              aggregation function.
        :param agg_func: The name of an aggregation function such as MAX, AVG.
        """
        self.time_interval = time_interval
        self.agg_op = agg_func

    def get_value(self, accumulator: ACC) -> T:
        return accumulator[0]

    def create_accumulator(self) -> ACC:
        return []

    def accumulate(self, accumulator: ACC, *args: pd.Series) -> None:
        df = pd.DataFrame({"val": args[0], "time": args[1]})
        latest_time = df["time"].iloc[-1]
        df = df[df.time >= latest_time - self.time_interval]
        accumulator.append(self.agg_op(df["val"]))


class _OverWindowDescriptor:
    """
    Descriptor of an over window.
    """

    def __init__(
        self,
        window_size: timedelta,
        limit: int,
        group_by_keys: Sequence[str],
        filter_expr: Optional[str],
    ) -> None:
        self.window_size = window_size
        self.limit = limit
        self.group_by_keys = group_by_keys
        self.filter_expr = filter_expr

    @staticmethod
    def from_window_agg_transform(
        window_agg_transform: WindowAggTransform,
    ) -> "_OverWindowDescriptor":
        return _OverWindowDescriptor(
            window_agg_transform.window_size,
            window_agg_transform.limit,
            window_agg_transform.group_by_keys,
            window_agg_transform.filter_expr,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.window_size == other.window_size
            and self.limit == other.limit
            and self.group_by_keys == other.group_by_keys
            and self.filter_expr == other.filter_expr
        )

    def __hash__(self) -> int:
        return hash(
            (self.window_size, self.limit, tuple(self.group_by_keys), self.filter_expr)
        )


class _AggregationDescriptor:
    """
    Descriptor of an aggregation operation.
    """

    def __init__(
        self,
        result_field_name: str,
        result_type: DataType,
        expr: str,
        agg_func: AggFunc,
    ) -> None:
        self.result_field_name = result_field_name
        self.result_type = result_type
        self.expr = expr
        self.agg_func = agg_func

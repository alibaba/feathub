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
from datetime import datetime, timedelta
from typing import Union, Optional, List, Any, Dict, Sequence, Tuple, Set, cast

import pandas as pd
from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
)
from pyflink.table.types import DataType

from feathub.common.exceptions import FeathubException, FeathubTransformationException
from feathub.common.types import DType
from feathub.common.utils import to_java_date_format
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    WINDOW_TIME_EXPR,
)
from feathub.feature_views.sql_feature_view import SqlFeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.over_window_transform import (
    OverWindowTransform,
)
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.flink_class_loader_utils import (
    ClassLoader,
)
from feathub.processors.flink.flink_types_utils import (
    to_flink_type,
    to_flink_schema,
    cast_field_type_without_changing_nullability,
)
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
    get_default_value_and_type,
)
from feathub.processors.flink.table_builder.flink_sql_expr_utils import (
    to_flink_sql_expr,
)
from feathub.processors.flink.table_builder.join_utils import (
    join_table_on_key,
    full_outer_join_on_key_with_default_value,
    temporal_join,
    JoinFieldDescriptor,
)
from feathub.processors.flink.table_builder.over_window_utils import (
    evaluate_over_window_transform,
    OverWindowDescriptor,
)
from feathub.processors.flink.table_builder.python_udf_utils import (
    evaluate_python_udf_transform,
)
from feathub.processors.flink.table_builder.redis_utils import (
    get_redis_source,
    lookup_join_redis_source,
)
from feathub.processors.flink.table_builder.sliding_window_utils import (
    evaluate_sliding_window_transform,
    SlidingWindowDescriptor,
)
from feathub.processors.flink.table_builder.source_sink_utils import (
    get_table_from_source,
)
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    define_watermark,
)
from feathub.processors.flink.table_builder.udf import register_all_feathub_udf
from feathub.registries.local_registry import LocalRegistry
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class FlinkTableBuilder:
    """FlinkTableBuilder is used to convert FeatHub feature to a Flink Table."""

    def __init__(
        self,
        t_env: StreamTableEnvironment,
        class_loader: ClassLoader,
        registry: Registry,
    ):
        """
        Instantiate the FlinkTableBuilder.

        :param t_env: The Flink StreamTableEnvironment under which the Tables to be
                      created.
        :param registry: The FeatHub registry.
        """
        self.t_env = t_env
        # TODO: PyFlink 1.16 and 1.17 have problem working with multiple
        #  StreamTableEnvironment. Creating a StreamTableEnvironment will overwrite
        #  the context class loader that belongs to the previous
        #  StreamTableEnvironment, which can cause class loading problem to the
        #  previous table environment. Therefore, we need to keep track of the
        #  classloader of each table environment. And set the context class loader
        #  accordingly when running operation on the table environment.
        #  This can be remove once https://issues.apache.org/jira/browse/FLINK-31943
        #  is resolved.
        self.class_loader = class_loader
        self.registry = registry

        # Mapping from the name of TableDescriptor to the TableDescriptor and the built
        # NativeFlinkTable. This is used as a cache to avoid re-computing the native
        # flink table from the same TableDescriptor.
        self._built_tables: Dict[str, Tuple[TableDescriptor, NativeFlinkTable]] = {}

        # Names of the TableDescriptors that are being built. This is used as a cache to
        # avoid recursively building the same table, so as to resolve potential circular
        # reference.
        self._tables_being_built: Set[str] = set()

        register_all_feathub_udf(self.t_env)

    def build(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        clear_built_tables: bool = True,
    ) -> NativeFlinkTable:
        """
        Convert the given features to native Flink table.

        If the given features is a FeatureView, it must be resolved, otherwise
        exception will be thrown.

        :param features: The feature to convert to native Flink table.
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
        :param clear_built_tables: Whether to clear the intermediate tables after
                                   building the Flink table. Set it to false when you
                                   want to build and materialize multiple tables in one
                                   Flink job. If it is false, user should call
                                   clear_built_tables after all tables are built.
        :return: The native Flink table that represents the given features.
        """
        with self.class_loader:
            try:
                native_flink_table = self._build(
                    features, keys, start_datetime, end_datetime
                )
            finally:
                if clear_built_tables:
                    self.clear_built_tables()
            return native_flink_table

    def clear_built_tables(self) -> None:
        self._built_tables.clear()

    def _build(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> NativeFlinkTable:
        if isinstance(features, FeatureView) and features.is_unresolved():
            raise FeathubException(
                "Trying to convert an unresolved FeatureView to native Flink table."
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

        if EVENT_TIME_ATTRIBUTE_NAME in table.get_schema().get_field_names():
            table = table.drop_columns(native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME))

        return table

    def _filter_table_by_keys(
        self,
        table: NativeFlinkTable,
        keys: Union[pd.DataFrame, TableDescriptor],
    ) -> NativeFlinkTable:
        key_table = self._get_table(keys)
        for field_name in key_table.get_schema().get_field_names():
            if field_name not in table.get_schema().get_field_names():
                raise FeathubException(
                    f"Given key {field_name} not in the table fields "
                    f"{table.get_schema().get_field_names()}."
                )
        return join_table_on_key(
            self.t_env, key_table, table, key_table.get_schema().get_field_names()
        )

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

        self._tables_being_built.add(features.name)

        if isinstance(features, FeatureTable):
            self._built_tables[features.name] = (
                features,
                get_table_from_source(self.t_env, features),
            )
        elif isinstance(features, DerivedFeatureView):
            self._built_tables[features.name] = (
                features,
                self._get_table_from_derived_feature_view(features),
            )
        elif isinstance(features, SlidingFeatureView):
            self._built_tables[features.name] = (
                features,
                self._get_table_from_sliding_feature_view(features),
            )
        elif isinstance(features, SqlFeatureView):
            self._built_tables[features.name] = (
                features,
                self._get_table_from_sql_feature_view(features),
            )
        else:
            raise FeathubException(
                f"Unsupported type '{type(features).__name__}' for '{features}'."
            )

        self._tables_being_built.remove(features.name)

        return self._built_tables[features.name][1]

    def _get_table_from_derived_feature_view(
        self, feature_view: DerivedFeatureView
    ) -> NativeFlinkTable:
        source_table = self._get_table(feature_view.source)
        source_fields = list(source_table.get_schema().get_field_names())
        dependent_features = self._get_dependent_features(feature_view)
        tmp_table = source_table

        window_agg_map: Dict[
            OverWindowDescriptor, List[AggregationFieldDescriptor]
        ] = {}

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
                    tmp_table = self._evaluate_expression_transform(
                        tmp_table,
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
                    tmp_table = evaluate_python_udf_transform(
                        tmp_table, feature.transform, feature.name, feature.dtype
                    )
            elif isinstance(feature.transform, OverWindowTransform):
                if feature_view.timestamp_field is None:
                    raise FeathubException(
                        "FeatureView must have timestamp field for OverWindowTransform."
                    )
                transform = feature.transform
                window_aggs = window_agg_map.setdefault(
                    OverWindowDescriptor.from_over_window_transform(transform),
                    [],
                )
                window_aggs.append(AggregationFieldDescriptor.from_feature(feature))
            elif isinstance(feature.transform, JoinTransform):
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
                # TODO: Remove this check after after Flink support terminate the
                #  job when the bounded left table finished.
                #  https://issues.apache.org/jira/projects/FLINK/issues/FLINK-30078
                # Raise exception if bounded left table join with an unbounded right
                # table.
                if (
                    feature_view.is_bounded()
                    and not right_table_descriptor.is_bounded()
                ):
                    raise FeathubException(
                        "Joining a bounded left table with an unbounded right table is "
                        "currently not supported. You can make the right table bounded "
                        "by setting its source to be bounded."
                    )
                right_table_join_field_descriptors = right_tables.setdefault(
                    (join_transform.table_name, tuple(feature.keys)), dict()
                )
                right_table_join_field_descriptors.update(
                    {
                        key: JoinFieldDescriptor.from_field_name(key)
                        for key in feature.keys
                    }
                )

                right_timestamp_field = right_table_descriptor.timestamp_field
                if right_timestamp_field is not None:
                    right_table_join_field_descriptors[
                        right_timestamp_field
                    ] = JoinFieldDescriptor.from_field_name(right_timestamp_field)

                    right_table_join_field_descriptors[
                        EVENT_TIME_ATTRIBUTE_NAME
                    ] = JoinFieldDescriptor.from_field_name(EVENT_TIME_ATTRIBUTE_NAME)

                right_table_join_field_descriptors[
                    feature.name
                ] = JoinFieldDescriptor.from_table_descriptor_and_feature(
                    right_table_descriptor, feature
                )
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        for (
            right_table_name,
            keys,
        ), right_table_join_field_descriptors in right_tables.items():
            right_table = table_by_names[right_table_name]
            right_table_descriptor = self.registry.get_features(right_table_name)
            redis_source = get_redis_source(right_table_descriptor)

            # TODO: Add Java implementation to evaluate Feathub expression so that
            #  Redis lookup source can directly process key_expr and remove the
            #  following codes.
            if redis_source is not None:
                tmp_table = lookup_join_redis_source(
                    t_env=self.t_env,
                    left_table=tmp_table,
                    right_table=right_table,
                    right_table_descriptor=right_table_descriptor,
                    join_field_descriptors=right_table_join_field_descriptors,
                    redis_source=redis_source,
                    keys=keys,
                )
                continue

            if right_table_descriptor.timestamp_field is None:
                raise FeathubException(
                    "FlinkProcessor can only perform join operation when right table "
                    "contains timestamp field or is from a lookup source like Redis."
                )

            right_table = right_table.select(
                *[
                    native_flink_expr.call_sql(
                        to_flink_sql_expr(descriptor.field_expr)
                    ).alias(name)
                    for name, descriptor in right_table_join_field_descriptors.items()
                ]
            )

            tmp_table = temporal_join(
                self.t_env,
                tmp_table,
                right_table,
                keys,
                right_table_join_field_descriptors,
            )

        for over_window_descriptor, agg_descriptor in window_agg_map.items():
            tmp_table = evaluate_over_window_transform(
                self.t_env,
                tmp_table,
                over_window_descriptor,
                agg_descriptor,
            )

        for feature in per_row_transform_features_following_first_over_window_or_join:
            if isinstance(feature.transform, ExpressionTransform):
                tmp_table = self._evaluate_expression_transform(
                    tmp_table,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            elif isinstance(feature.transform, PythonUdfTransform):
                tmp_table = evaluate_python_udf_transform(
                    tmp_table,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type: {type(feature.transform)}."
                )

        tmp_table = self._apply_filter_if_any(tmp_table, feature_view.filter_expr)

        output_fields = self._get_output_fields(feature_view, source_fields)
        return tmp_table.select(
            *[native_flink_expr.col(field) for field in output_fields]
        )

    def _get_table_from_sliding_feature_view(
        self, feature_view: SlidingFeatureView
    ) -> NativeFlinkTable:
        source_table = self._get_table(feature_view.source)
        source_fields = source_table.get_schema().get_field_names()

        dependent_features = self._get_dependent_features(feature_view)

        tmp_table = source_table
        sliding_window_agg_map: Dict[
            SlidingWindowDescriptor, List[AggregationFieldDescriptor]
        ] = {}

        # This list contains all per-row transform features listed after the first
        # SlidingWindowTransform feature in the dependent_features.
        per_row_transform_features_following_first_sliding_feature = []

        for feature in dependent_features:
            if isinstance(feature.transform, ExpressionTransform):
                if len(sliding_window_agg_map) > 0:
                    per_row_transform_features_following_first_sliding_feature.append(
                        feature
                    )
                else:
                    tmp_table = self._evaluate_expression_transform(
                        tmp_table,
                        feature.transform,
                        feature.name,
                        feature.dtype,
                    )
            elif isinstance(feature.transform, PythonUdfTransform):
                if len(sliding_window_agg_map) > 0:
                    per_row_transform_features_following_first_sliding_feature.append(
                        feature
                    )
                else:
                    tmp_table = evaluate_python_udf_transform(
                        tmp_table,
                        feature.transform,
                        feature.name,
                        feature.dtype,
                    )
            elif isinstance(feature.transform, SlidingWindowTransform):
                if feature_view.timestamp_field is None:
                    raise FeathubException(
                        "SlidingFeatureView must have timestamp field for "
                        "SlidingWindowTransform."
                    )
                transform = feature.transform
                window_aggs = sliding_window_agg_map.setdefault(
                    SlidingWindowDescriptor.from_sliding_window_transform(transform),
                    [],
                )
                window_aggs.append(AggregationFieldDescriptor.from_feature(feature))
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        agg_table = None
        field_default_value: Dict[str, Tuple[Any, DataType]] = {}
        for window_descriptor, agg_descriptors in sliding_window_agg_map.items():
            for agg_descriptor in agg_descriptors:
                field_default_value[
                    agg_descriptor.field_name
                ] = get_default_value_and_type(agg_descriptor)
            tmp_agg_table = evaluate_sliding_window_transform(
                self.t_env,
                tmp_table,
                window_descriptor,
                agg_descriptors,
                feature_view.config,
            )
            if agg_table is None:
                agg_table = tmp_agg_table
            else:
                join_keys = list(window_descriptor.group_by_keys)
                join_keys.append(EVENT_TIME_ATTRIBUTE_NAME)
                agg_table = full_outer_join_on_key_with_default_value(
                    self.t_env,
                    agg_table,
                    tmp_agg_table,
                    join_keys,
                    field_default_value,
                )

        if agg_table is not None:
            tmp_table = agg_table

        # Add the timestamp field according to the timestamp format from
        # event time(window time).
        if feature_view.timestamp_field is not None:
            if feature_view.timestamp_format == "epoch":
                tmp_table = tmp_table.add_columns(
                    native_flink_expr.call_sql(
                        f"UNIX_TIMESTAMP(CAST(`{EVENT_TIME_ATTRIBUTE_NAME}` "
                        f"AS STRING))"
                    ).alias(feature_view.timestamp_field)
                )
            elif feature_view.timestamp_format == "epoch_millis":
                tmp_table = tmp_table.add_columns(
                    native_flink_expr.call_sql(
                        f"UNIX_TIMESTAMP_MILLIS(CAST(`{EVENT_TIME_ATTRIBUTE_NAME}` "
                        f"AS STRING), '{self.t_env.get_config().get_local_timezone()}')"
                    ).alias(feature_view.timestamp_field)
                )
            else:
                java_datetime_format = to_java_date_format(
                    feature_view.timestamp_format
                ).replace(
                    "'", "''"  # Escape single quote for sql
                )
                tmp_table = tmp_table.add_columns(
                    native_flink_expr.call_sql(
                        f"DATE_FORMAT(`{EVENT_TIME_ATTRIBUTE_NAME}`, "
                        f"'{java_datetime_format}')"
                    ).alias(feature_view.timestamp_field)
                )

        for feature in per_row_transform_features_following_first_sliding_feature:
            if isinstance(feature.transform, ExpressionTransform):
                if feature.transform.expr == WINDOW_TIME_EXPR:
                    continue
                tmp_table = self._evaluate_expression_transform(
                    tmp_table,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            elif isinstance(feature.transform, PythonUdfTransform):
                tmp_table = evaluate_python_udf_transform(
                    tmp_table,
                    feature.transform,
                    feature.name,
                    feature.dtype,
                )
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type: {type(feature.transform)}."
                )

        tmp_table = self._apply_filter_if_any(tmp_table, feature_view.filter_expr)

        output_fields = self._get_output_fields(feature_view, source_fields)
        return tmp_table.select(
            *[native_flink_expr.col(field) for field in output_fields]
        )

    def _get_table_from_sql_feature_view(
        self, feature_view: SqlFeatureView
    ) -> NativeFlinkTable:
        for table_name in self._get_all_registered_table_names():
            if (
                table_name in self.t_env.list_temporary_views()
                or table_name in self._tables_being_built
            ):
                continue
            self.t_env.create_temporary_view(
                table_name, self._get_table(self.registry.get_features(table_name))
            )

        result_table = self.t_env.sql_query(feature_view.sql_statement)

        if feature_view.timestamp_field is not None:
            result_stream = self.t_env.to_data_stream(result_table)

            # TODO: add support for max_out_of_orderness in SqlFeatureView.
            result_table = self.t_env.from_data_stream(
                result_stream,
                define_watermark(
                    t_env=self.t_env,
                    flink_schema=to_flink_schema(feature_view.schema),
                    max_out_of_orderness=timedelta(0),
                    timestamp_field=feature_view.timestamp_field,
                    timestamp_format=feature_view.timestamp_format,
                    timestamp_field_dtype=feature_view.schema.get_field_type(
                        feature_view.timestamp_field
                    ),
                ),
            )

        return result_table

    def _get_all_registered_table_names(self) -> Set[str]:
        if isinstance(self.registry, LocalRegistry):
            return set(x for x in cast(LocalRegistry, self.registry).tables.keys())

        # Non-local registries do not support scanning all registered tables. Make sure
        # that all needed features have been properly configures through other paths.
        return set()

    @staticmethod
    def _apply_filter_if_any(
        table: NativeFlinkTable, filter_expr: Optional[str]
    ) -> NativeFlinkTable:
        # TODO: Apply filtering as early as possible to improve performance.
        if filter_expr is None:
            return table
        return table.filter(native_flink_expr.call_sql(to_flink_sql_expr(filter_expr)))

    @staticmethod
    def _get_dependent_features(feature_view: FeatureView) -> List[Feature]:
        dependent_features = []
        for feature in feature_view.get_resolved_features():
            for input_feature in feature.input_features:
                if input_feature not in dependent_features:
                    dependent_features.append(input_feature)
            if feature not in dependent_features:
                dependent_features.append(feature)
        return dependent_features

    @staticmethod
    def _evaluate_expression_transform(
        source_table: NativeFlinkTable,
        transform: ExpressionTransform,
        result_field_name: str,
        result_type: DType,
    ) -> NativeFlinkTable:
        result_type = to_flink_type(result_type)
        table = source_table.add_or_replace_columns(
            native_flink_expr.call_sql(to_flink_sql_expr(transform.expr)).alias(
                result_field_name
            )
        )
        return cast_field_type_without_changing_nullability(
            table, {result_field_name: result_type}
        )

    def _range_table_by_time(
        self,
        table: NativeFlinkTable,
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
    ) -> NativeFlinkTable:
        if start_datetime is not None:
            table = table.filter(
                native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME).__ge__(
                    native_flink_expr.to_timestamp_ltz(
                        int(start_datetime.timestamp() * 1000), 3
                    )
                )
            )
        if end_datetime is not None:
            table = table.filter(
                native_flink_expr.col(EVENT_TIME_ATTRIBUTE_NAME).__lt__(
                    native_flink_expr.to_timestamp_ltz(
                        int(end_datetime.timestamp() * 1000), 3
                    )
                )
            )
        return table

    def _get_output_fields(
        self, feature_view: FeatureView, source_fields: List[str]
    ) -> List[str]:
        output_fields = feature_view.get_output_fields(source_fields)
        if (
            EVENT_TIME_ATTRIBUTE_NAME not in output_fields
            and feature_view.timestamp_field is not None
        ):
            output_fields.append(EVENT_TIME_ATTRIBUTE_NAME)
        return output_fields

    @staticmethod
    def _get_feature_valid_time(
        table_descriptor: TableDescriptor, feature_name: str
    ) -> Optional[timedelta]:
        if not isinstance(table_descriptor, SlidingFeatureView):
            return None

        right_feature_transform = table_descriptor.get_feature(feature_name).transform

        if not isinstance(right_feature_transform, SlidingWindowTransform):
            return None

        return right_feature_transform.step_size

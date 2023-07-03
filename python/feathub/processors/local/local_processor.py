# Copyright 2022 The FeatHub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import (
    Dict,
    Optional,
    Union,
    List,
    Any,
    Sequence,
)

import pandas as pd
from dateutil.tz import tz

import feathub.common.utils as utils
from feathub.common.config import TIMEZONE_CONFIG
from feathub.common.exceptions import FeathubException, FeathubTransformationException
from feathub.common.types import to_numpy_dtype
from feathub.dsl.expr_parser import ExprParser
from feathub.dsl.expr_utils import is_id, get_var_name
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.memory_store_sink import MemoryStoreSink
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.over_window_transform import (
    OverWindowTransform,
)
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.local.aggregation_utils import AGG_FUNCTIONS
from feathub.processors.local.ast_evaluator.local_ast_evaluator import LocalAstEvaluator
from feathub.processors.local.file_system_utils import (
    insert_into_file_sink,
    get_dataframe_from_file_source,
)
from feathub.processors.local.local_job import LocalJob
from feathub.processors.local.local_processor_config import LocalProcessorConfig
from feathub.processors.local.local_table import LocalTable
from feathub.processors.local.sliding_window_utils import (
    SlidingWindowDescriptor,
    AggregationFieldDescriptor,
    evaluate_sliding_window,
)
from feathub.processors.local.time_utils import (
    append_and_sort_unix_time_column,
    append_unix_time_column,
)
from feathub.processors.processor import (
    Processor,
)
from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.processors.processor_job import ProcessorJob
from feathub.processors.type_utils import cast_series_dtype
from feathub.registries.registry import Registry
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


def _is_spark_supported_source(source: FeatureTable) -> bool:
    return isinstance(source, (FileSystemSource, DataGenSource))


def _is_spark_supported_sink(sink: Sink) -> bool:
    return isinstance(
        sink,
        (
            FileSystemSink,
            PrintSink,
            BlackHoleSink,
            MemoryStoreSink,
        ),
    )


class LocalProcessor(Processor):
    """
    A LocalProcessor uses CPUs on the local machine to compute features and uses Pandas
    DataFrame to store tabular data in memory.
    """

    def __init__(self, props: Dict, registry: Registry):
        """
        :param props: The processor properties.
        :param registry: An entity registry.
        """
        super().__init__()
        self.props = props
        self.registry = registry

        self.config = LocalProcessorConfig(props)
        self.timezone = tz.gettz(self.config.get(TIMEZONE_CONFIG))

        self.parser = ExprParser()
        self.ast_evaluator = LocalAstEvaluator(tz=self.timezone)

        self.spark_session: Optional[Any] = None
        self.executor = ThreadPoolExecutor()

    def get_table(
        self,
        feature_descriptor: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> LocalTable:
        feature_descriptor = self._resolve_table_descriptor(feature_descriptor)
        df = self._get_table(feature_descriptor).df
        if keys is not None:
            if not isinstance(keys, pd.DataFrame):
                keys = self._get_table(keys).df
            if not set(keys.columns).issubset(set(df.columns)):
                raise FeathubException(
                    f"Not all given key {keys.columns} in the table fields "
                    f"{df.columns}."
                )
            keys = keys.drop_duplicates()
            idx = df[list(keys.columns)].apply(
                lambda row: any([row.equals(key) for _, key in keys.iterrows()]),
                axis=1,
            )
            df = df[idx]

        if start_datetime is not None or end_datetime is not None:
            if feature_descriptor.timestamp_field is None:
                raise FeathubException("Features do not have timestamp column.")
            if feature_descriptor.timestamp_format is None:
                raise FeathubException("Features do not have timestamp format.")
            append_and_sort_unix_time_column(
                df,
                feature_descriptor.timestamp_field,
                feature_descriptor.timestamp_format,
                self.timezone,
            )
        if start_datetime is not None:
            unix_start_datetime = utils.to_unix_timestamp(
                start_datetime, tz=self.timezone
            )
            df = df[df[EVENT_TIME_ATTRIBUTE_NAME] >= unix_start_datetime]
        if end_datetime is not None:
            unix_end_datetime = utils.to_unix_timestamp(end_datetime, tz=self.timezone)
            df = df[df[EVENT_TIME_ATTRIBUTE_NAME] < unix_end_datetime]
        if EVENT_TIME_ATTRIBUTE_NAME in df:
            df = df.drop(columns=[EVENT_TIME_ATTRIBUTE_NAME])

        return LocalTable(
            processor=self,
            features=feature_descriptor,
            df=df.reset_index(drop=True),
            timestamp_field=feature_descriptor.timestamp_field,
            timestamp_format=feature_descriptor.timestamp_format,
        )

    def materialize_features(
        self,
        materialization_descriptors: Sequence[MaterializationDescriptor],
    ) -> ProcessorJob:
        for materialization_descriptor in materialization_descriptors:
            if (
                materialization_descriptor.ttl is not None
                or not materialization_descriptor.allow_overwrite
            ):
                raise RuntimeError("Unsupported operation.")
            feature_descriptor = self._resolve_table_descriptor(
                materialization_descriptor.feature_descriptor
            )

            features_df = self.get_table(
                feature_descriptor=feature_descriptor,
                keys=None,
                start_datetime=materialization_descriptor.start_datetime,
                end_datetime=materialization_descriptor.end_datetime,
            ).to_pandas()

            self.materialize_dataframe(
                features=feature_descriptor,
                features_df=features_df,
                sink=materialization_descriptor.sink,
                allow_overwrite=materialization_descriptor.allow_overwrite,
            )

        return LocalJob()

    def materialize_dataframe(
        self,
        features: TableDescriptor,
        features_df: pd.DataFrame,
        sink: Sink,
        allow_overwrite: bool = False,
    ) -> LocalJob:

        # TODO: handle allow_overwrite.
        if isinstance(sink, MemoryStoreSink):
            if features.keys is None:
                raise FeathubException(f"Features keys must not be None {features}.")
            return self._write_features_to_online_store(
                features=features_df,
                schema=utils.get_table_schema(features),
                sink=sink,
                key_fields=features.keys,
                timestamp_field=features.timestamp_field,
                timestamp_format=features.timestamp_format,
            )
        elif isinstance(sink, FileSystemSink) and utils.is_local_file_or_dir(sink.path):
            insert_into_file_sink(features_df, sink)
            return LocalJob()
        elif _is_spark_supported_sink(sink):
            return self._materialize_dataframe_using_spark(
                df=features_df,
                features=features,
                sink=sink,
                allow_overwrite=allow_overwrite,
            )

        raise RuntimeError(f"Unsupported sink: {sink}.")

    def _get_table(self, features: Union[str, TableDescriptor]) -> LocalTable:
        if isinstance(features, str):
            raise FeathubException(
                f"Cannot get LocalTable from unresolved features {features}."
            )

        if isinstance(features, FileSystemSource) and utils.is_local_file_or_dir(
            features.path
        ):
            return self._get_table_from_file_source(features)
        elif isinstance(features, DerivedFeatureView):
            return self._get_table_from_derived_feature_view(features)
        elif isinstance(features, SlidingFeatureView):
            return self._get_table_from_sliding_feature_view(features)
        elif isinstance(features, FeatureTable) and _is_spark_supported_source(
            features
        ):
            return self._get_table_using_spark(features)

        raise FeathubException(
            f"Unsupported type '{type(features).__name__}' for '{features}'."
        )

    def _write_features_to_online_store(
        self,
        features: pd.DataFrame,
        schema: Schema,
        sink: MemoryStoreSink,
        key_fields: List[str],
        timestamp_field: Optional[str],
        timestamp_format: Optional[str],
    ) -> LocalJob:
        MemoryOnlineStore.get_instance().put(
            table_name=sink.table_name,
            features=features,
            schema=schema,
            key_fields=key_fields,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

        return LocalJob()

    def _get_table_from_file_source(self, source: FileSystemSource) -> LocalTable:
        df = get_dataframe_from_file_source(source)
        return LocalTable(
            processor=self,
            features=source,
            df=df,
            timestamp_field=source.timestamp_field,
            timestamp_format=source.timestamp_format,
        )

    def _get_table_using_spark(self, source: FeatureTable) -> LocalTable:
        try:
            self._init_spark_session_local_mode()
        except ImportError:
            raise FeathubException(
                f"Please install Feathub with Spark to use {source} "
                f"in LocalProcessor."
            )

        from feathub.processors.spark.dataframe_builder import (
            source_sink_utils as spark_source_sink_utils,
        )

        spark_dataframe = spark_source_sink_utils.get_dataframe_from_source(
            spark_session=self.spark_session,
            source=source,
        )
        df = spark_dataframe.toPandas()
        return LocalTable(
            processor=self,
            features=source,
            df=df,
            timestamp_field=source.timestamp_field,
            timestamp_format=source.timestamp_format,
        )

    def _materialize_dataframe_using_spark(
        self,
        df: pd.DataFrame,
        features: TableDescriptor,
        sink: Sink,
        allow_overwrite: bool = False,
    ) -> LocalJob:
        try:
            self._init_spark_session_local_mode()
        except ImportError:
            raise FeathubException(
                f"Please install Feathub with Spark to use {sink} "
                f"in LocalProcessor."
            )

        from feathub.processors.spark.dataframe_builder import (
            source_sink_utils as spark_source_sink_utils,
        )

        spark_dataframe = self.spark_session.createDataFrame(df)
        spark_source_sink_utils.insert_into_sink(
            executor=self.executor,
            dataframe=spark_dataframe,
            features_desc=features,
            sink=sink,
            allow_overwrite=allow_overwrite,
        ).result()
        return LocalJob()

    def _evaluate_expression_transform(
        self, df: pd.DataFrame, transform: ExpressionTransform
    ) -> List:
        expr_node = self.parser.parse(transform.expr)
        return df.apply(
            lambda row: self.ast_evaluator.eval(expr_node, row), axis=1
        ).tolist()

    def _get_table_from_derived_feature_view(
        self, feature_view: DerivedFeatureView
    ) -> LocalTable:

        source_table = self._get_table(feature_view.source)
        source_df = source_table.df
        source_fields = list(source_table.get_schema().field_names)
        dependent_features = self._get_dependent_features(feature_view)

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

        for feature in dependent_features:
            if isinstance(feature.transform, ExpressionTransform):
                source_df[feature.name] = self._evaluate_expression_transform(
                    source_df, feature.transform
                )
            elif isinstance(feature.transform, PythonUdfTransform):
                source_df[feature.name] = self._evaluate_python_udf_transform(
                    source_df, feature.transform
                )
            elif isinstance(feature.transform, OverWindowTransform):
                if (
                    feature_view.timestamp_field is None
                    or feature_view.timestamp_format is None
                ):
                    raise FeathubException(
                        "FeatureView must have timestamp field and timestamp format "
                        "specified for OverWindowTransform."
                    )
                source_df[feature.name] = self._evaluate_over_window_transform(
                    source_df,
                    feature.transform,
                    feature_view.timestamp_field,
                    feature_view.timestamp_format,
                )
            elif isinstance(feature.transform, JoinTransform):
                source_df[feature.name] = self._evaluate_join_transform(
                    source_df,
                    feature,
                    feature_view.timestamp_field,
                    feature_view.timestamp_format,
                    table_by_names,
                    descriptors_by_names,
                )
            else:
                raise RuntimeError(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )
            source_df[feature.name] = cast_series_dtype(
                source_df[feature.name], to_numpy_dtype(feature.dtype)
            )

        if feature_view.filter_expr is not None:
            source_df = self._filter_dataframe(source_df, feature_view.filter_expr)

        output_fields = feature_view.get_output_fields(source_fields)

        return LocalTable(
            processor=self,
            features=feature_view,
            df=source_df[output_fields],
            timestamp_field=feature_view.timestamp_field,
            timestamp_format=feature_view.timestamp_format,
        )

    def _resolve_table_descriptor(
        self, features: Union[str, TableDescriptor]
    ) -> TableDescriptor:
        if isinstance(features, str):
            features = self.registry.get_features(name=features)
        elif isinstance(features, FeatureView) and features.is_unresolved():
            features = self.registry.build_features([features])[0]

        return features

    def _evaluate_join_transform(
        self,
        source_df: pd.DataFrame,
        feature: Feature,
        source_timestamp_field: str,
        source_timestamp_format: str,
        table_by_names: Dict[str, LocalTable],
        descriptors_by_names: Dict[str, TableDescriptor],
    ) -> List:
        if feature.keys is None:
            raise FeathubException(
                f"Feature {feature} with JoinTransform must have keys."
            )
        join_transform = feature.transform
        if not isinstance(join_transform, JoinTransform):
            raise RuntimeError(f"Feature '{feature.name}' should use JoinTransform.")

        if not is_id(join_transform.expr):
            raise FeathubException(
                "It is not supported to use Feathub expression in JoinTransform for "
                "local processor."
            )

        join_descriptor = descriptors_by_names[join_transform.table_name]
        if (
            join_descriptor.timestamp_field is None
            or join_descriptor.timestamp_format is None
        ):
            raise FeathubException(
                "Join table must have timestamp field and timestamp format specified."
            )
        join_timestamp_field = join_descriptor.timestamp_field
        join_timestamp_format = join_descriptor.timestamp_format
        join_df = table_by_names[join_transform.table_name].df
        join_feature = join_descriptor.get_feature(get_var_name(join_transform.expr))
        if join_feature.keys is None:
            raise FeathubException(
                f"The Feature {join_feature} to join must have keys."
            )

        result = []
        # TODO: optimize the performance for the following code.
        for source_idx, source_row in source_df.iterrows():
            source_timestamp = utils.to_unix_timestamp(
                source_row[source_timestamp_field],
                source_timestamp_format,
                self.timezone,
            )
            joined_value = None
            joined_timestamp = None
            for join_idx, join_row in join_df.iterrows():
                join_timestamp = utils.to_unix_timestamp(
                    join_row[join_timestamp_field], join_timestamp_format, self.timezone
                )
                if join_timestamp > source_timestamp:
                    continue
                if joined_timestamp is not None and joined_timestamp >= join_timestamp:
                    continue

                keys_match = True
                for i in range(len(feature.keys)):
                    if source_row[feature.keys[i]] != join_row[join_feature.keys[i]]:
                        keys_match = False
                        break
                if not keys_match:
                    continue
                joined_value = join_row[get_var_name(join_transform.expr)]
                joined_timestamp = join_timestamp
            result.append(joined_value)

        return result

    def _evaluate_over_window_transform(
        self,
        df: pd.DataFrame,
        transform: OverWindowTransform,
        timestamp_field: str,
        timestamp_format: str,
    ) -> List:
        agg_func = AGG_FUNCTIONS.get(transform.agg_func, None)
        if agg_func is None:
            raise RuntimeError(f"Unsupported agg function {transform.agg_func}.")
        temp_column = "_temp"
        if temp_column in df:
            raise RuntimeError("The dataframe has column with name _temp.")

        for key in transform.group_by_keys:
            if key not in df:
                raise RuntimeError(
                    f"Group-by key '{key}' is not found in {df.columns}."
                )

        expr_node = self.parser.parse(transform.expr)
        df_copy = df.copy()
        df_copy[temp_column] = df_copy.apply(
            lambda row: self.ast_evaluator.eval(expr_node, row), axis=1
        )

        # Append an internal unix time column.
        append_unix_time_column(
            df_copy, timestamp_field, timestamp_format, self.timezone
        )

        group_by_idx = {}
        if len(transform.group_by_keys) > 0:
            for group in df_copy.groupby(transform.group_by_keys).indices.values():
                for idx in group:
                    group_by_idx[idx] = group

        filter_expr_node = None
        if transform.filter_expr is not None:
            filter_expr_node = self.parser.parse(transform.filter_expr)
        result: List[Any] = []
        # TODO: optimize the performance for the following code.
        # Computes the feature's value for each row in the group.
        for idx, row in df_copy.iterrows():
            if filter_expr_node is not None and not self.ast_evaluator.eval(
                filter_expr_node, dict(row)
            ):
                result.append(None)
                continue
            max_timestamp = row[EVENT_TIME_ATTRIBUTE_NAME]
            window_size = transform.window_size
            min_timestamp = (
                0
                if window_size is None
                else max_timestamp - window_size.total_seconds()
            )

            rows_in_group = (
                # If group_by_idx is empty all rows in the same group.
                df_copy.iloc[group_by_idx[idx]]
                if len(group_by_idx) > 0
                else df_copy
            )
            predicate = rows_in_group[EVENT_TIME_ATTRIBUTE_NAME].transform(
                lambda timestamp: min_timestamp <= timestamp <= max_timestamp
            )
            if filter_expr_node is not None:
                predicate = predicate & rows_in_group.apply(
                    lambda r: self.ast_evaluator.eval(filter_expr_node, r.to_dict()),
                    axis=1,
                )
            rows_in_group_and_window = rows_in_group[predicate]
            limit = transform.limit
            if limit is not None:
                rows_in_group_and_window.sort_values(
                    by=[EVENT_TIME_ATTRIBUTE_NAME],
                    ascending=True,
                    inplace=True,
                    ignore_index=True,
                )
                rows_in_group_and_window = rows_in_group_and_window.iloc[-limit:]
            result.append(agg_func(rows_in_group_and_window[temp_column].tolist()))

        return result

    def _evaluate_python_udf_transform(
        self, df: pd.DataFrame, transform: PythonUdfTransform
    ) -> List:
        return df.apply(lambda row: transform.udf(row), axis=1).tolist()

    def _get_table_from_sliding_feature_view(
        self, feature_view: SlidingFeatureView
    ) -> LocalTable:
        if (
            feature_view.config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG) is not True
            and feature_view.config.get(SKIP_SAME_WINDOW_OUTPUT_CONFIG) is not True
        ):
            raise FeathubException(
                "LocalProcessor only supports sliding window with "
                "ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG = True and "
                "SKIP_SAME_WINDOW_OUTPUT_CONFIG = True."
            )
        source_table = self._get_table(feature_view.source)
        source_df = source_table.df
        source_fields = list(source_table.get_schema().field_names)
        dependent_features = self._get_dependent_features(feature_view)

        sliding_window_descriptor: Optional[SlidingWindowDescriptor] = None
        agg_field_descriptors: List[AggregationFieldDescriptor] = []

        # This list contains all per-row transform features listed after the first
        # SlidingWindowTransform feature in the dependent_features.
        per_row_transform_features_following_first_sliding_feature = []

        for feature in dependent_features:

            # The timestamp field is computed as part of the sliding window.
            if feature.name == feature_view.timestamp_field:
                continue

            if isinstance(feature.transform, ExpressionTransform):
                if sliding_window_descriptor is not None:
                    per_row_transform_features_following_first_sliding_feature.append(
                        feature
                    )
                else:
                    source_df[feature.name] = self._evaluate_expression_transform(
                        source_df, feature.transform
                    )
            elif isinstance(feature.transform, PythonUdfTransform):
                if sliding_window_descriptor is not None:
                    per_row_transform_features_following_first_sliding_feature.append(
                        feature
                    )
                else:
                    source_df[feature.name] = self._evaluate_python_udf_transform(
                        source_df, feature.transform
                    )
            elif isinstance(feature.transform, SlidingWindowTransform):
                if feature_view.timestamp_field is None:
                    raise FeathubException(
                        "SlidingFeatureView must have timestamp field for "
                        "SlidingWindowTransform."
                    )
                transform = feature.transform

                if sliding_window_descriptor is None:
                    sliding_window_descriptor = (
                        SlidingWindowDescriptor.from_sliding_window_transform(transform)
                    )

                if (
                    sliding_window_descriptor
                    != SlidingWindowDescriptor.from_sliding_window_transform(transform)
                ):
                    raise FeathubException(
                        "The SlidingWindowTransforms in a SlidingFeatureView should "
                        "have the same step size and group by keys."
                    )

                agg_field_descriptors.append(
                    AggregationFieldDescriptor.from_feature(feature)
                )
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type "
                    f"{type(feature.transform).__name__} for feature {feature.name}."
                )

        agg_df = evaluate_sliding_window(
            input_df=source_df,
            feature_view=feature_view,
            window_descriptor=sliding_window_descriptor,
            agg_descriptors=agg_field_descriptors,
            tz=self.timezone,
            parser=self.parser,
            ast_evaluator=self.ast_evaluator,
        )

        for feature in per_row_transform_features_following_first_sliding_feature:
            if isinstance(feature.transform, ExpressionTransform):
                agg_df[feature.name] = self._evaluate_expression_transform(
                    agg_df, feature.transform
                )
            elif isinstance(feature.transform, PythonUdfTransform):
                agg_df[feature.name] = self._evaluate_python_udf_transform(
                    agg_df, feature.transform
                )
            else:
                raise FeathubTransformationException(
                    f"Unsupported transformation type: {type(feature.transform)}."
                )

        if feature_view.filter_expr is not None:
            agg_df = self._filter_dataframe(agg_df, feature_view.filter_expr)

        output_fields = feature_view.get_output_fields(source_fields)

        return LocalTable(
            processor=self,
            features=feature_view,
            df=agg_df[output_fields],
            timestamp_field=feature_view.timestamp_field,
            timestamp_format=feature_view.timestamp_format,
        )

    def _get_dependent_features(self, feature_view: FeatureView) -> Sequence[Feature]:
        dependent_features = []
        for feature in feature_view.get_resolved_features():
            for input_feature in feature.input_features:
                if input_feature not in dependent_features:
                    dependent_features.append(input_feature)
            if feature not in dependent_features:
                dependent_features.append(feature)
        return dependent_features

    def _filter_dataframe(self, df: pd.DataFrame, filter_expr: str) -> pd.DataFrame:
        filter_ast = self.parser.parse(filter_expr)
        return df[df.apply(lambda r: self.ast_evaluator.eval(filter_ast, r), axis=1)]

    def _init_spark_session_local_mode(self) -> None:
        if self.spark_session is not None:
            return

        from pyspark.sql import SparkSession

        spark_session_builder = SparkSession.builder
        spark_session_builder = spark_session_builder.master("local[*]")
        spark_session_builder = spark_session_builder.config(
            "spark.sql.session.timeZone", self.config.get(TIMEZONE_CONFIG)
        )
        self.spark_session = spark_session_builder.getOrCreate()

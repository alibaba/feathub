# Copyright 2022 The Feathub Authors
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

import pandas as pd
import numpy as np
from typing import Dict, Optional, Union, List, Any
from datetime import datetime, timedelta

import feathub.common.utils as utils
from feathub.common.exceptions import FeathubException
from feathub.common.types import to_numpy_dtype
from feathub.dsl.expr_parser import ExprParser
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.processors.local.ast_evaluator.local_ast_evaluator import LocalAstEvaluator
from feathub.processors.processor import Processor
from feathub.registries.registry import Registry
from feathub.processors.local.local_job import LocalJob
from feathub.feature_tables.sinks.memory_store_sink import MemoryStoreSink
from feathub.processors.local.local_table import LocalTable
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.over_window_transform import (
    OverWindowTransform,
)
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.table.table_descriptor import TableDescriptor
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.feature import Feature
from feathub.online_stores.memory_online_store import MemoryOnlineStore


class LocalProcessor(Processor):
    """
    A LocalProcessor uses CPUs on the local machine to compute features and uses Pandas
    DataFrame to store tabular data in memory.
    """

    _AGG_FUNCTIONS = {
        AggFunc.AVG: np.mean,
        AggFunc.SUM: np.sum,
        AggFunc.MAX: np.max,
        AggFunc.MIN: np.min,
        AggFunc.FIRST_VALUE: lambda l: l[0],
        AggFunc.LAST_VALUE: lambda l: l[-1],
        AggFunc.ROW_NUMBER: lambda l: len(l),
        AggFunc.VALUE_COUNTS: lambda l: dict(pd.Series(l).value_counts()),
    }

    def __init__(self, props: Dict, registry: Registry):
        """
        :param props: The processor properties.
        :param registry: An entity registry.
        """
        super().__init__()
        self.props = props
        self.registry = registry
        self.parser = ExprParser()
        self.ast_evaluator = LocalAstEvaluator()

    def get_table(
        self,
        features: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> LocalTable:
        features = self._resolve_table_descriptor(features)
        df = self._get_table(features).df
        if keys is not None:
            keys = self._get_table(keys).df
            keys = keys.drop_duplicates()
            idx = df[list(keys.columns)].apply(
                lambda row: any([row.equals(key) for _, key in keys.iterrows()]),
                axis=1,
            )
            df = df[idx]

        unix_time_column = None
        if start_datetime is not None or end_datetime is not None:
            if features.timestamp_field is None:
                raise FeathubException("Features do not have timestamp column.")
            if features.timestamp_format is None:
                raise FeathubException("Features do not have timestamp format.")
            unix_time_column = utils.append_and_sort_unix_time_column(
                df, features.timestamp_field, features.timestamp_format
            )
        if start_datetime is not None:
            unix_start_datetime = utils.to_unix_timestamp(start_datetime)
            df = df[df[unix_time_column] >= unix_start_datetime]
        if end_datetime is not None:
            unix_end_datetime = utils.to_unix_timestamp(end_datetime)
            df = df[df[unix_time_column] < unix_end_datetime]
        if unix_time_column is not None:
            df = df.drop(columns=[unix_time_column])

        return LocalTable(
            df=df.reset_index(drop=True),
            timestamp_field=features.timestamp_field,
            timestamp_format=features.timestamp_format,
        )

    # TODO: figure out whether and how to support long running feature materialization.
    def materialize_features(
        self,
        features: Union[str, TableDescriptor],
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ) -> LocalJob:
        # TODO: support ttl
        if ttl is not None or not allow_overwrite:
            raise RuntimeError("Unsupported operation.")
        features = self._resolve_table_descriptor(features)
        if features.keys is None:
            raise FeathubException(f"Features keys must not be None {features}.")

        features_df = self.get_table(
            features=features,
            keys=None,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        ).to_pandas()

        # TODO: handle allow_overwrite.
        # TODO: Support FileSystemSink, KafkaSink, PrintSink.
        if isinstance(sink, MemoryStoreSink):
            return self._write_features_to_online_store(
                features=features_df,
                sink=sink,
                key_fields=features.keys,
                timestamp_field=features.timestamp_field,
                timestamp_format=features.timestamp_format,
            )

        raise RuntimeError(f"Unsupported sink: {sink}.")

    def _get_table(self, features: Union[pd.DataFrame, TableDescriptor]) -> LocalTable:
        if isinstance(features, pd.DataFrame):
            return LocalTable(
                df=features,
                timestamp_field=None,
                timestamp_format="unknown",
            )

        # TODO: Support SlidingFeatureView, KafkaSource, DataGenSource.
        if isinstance(features, FileSystemSource):
            return self._get_table_from_file_source(features)
        elif isinstance(features, DerivedFeatureView):
            return self._get_table_from_derived_feature_view(features)

        raise RuntimeError(
            f"Unsupported type '{type(features).__name__}' for '{features}'."
        )

    def _write_features_to_online_store(
        self,
        features: pd.DataFrame,
        sink: MemoryStoreSink,
        key_fields: List[str],
        timestamp_field: Optional[str],
        timestamp_format: Optional[str],
    ) -> LocalJob:
        MemoryOnlineStore.get_instance().put(
            table_name=sink.table_name,
            features=features,
            key_fields=key_fields,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

        return LocalJob()

    def _get_table_from_file_source(self, source: FileSystemSource) -> LocalTable:
        if source.data_format == "csv":
            df = pd.read_csv(
                source.path,
                names=source.schema.field_names,
                dtype={
                    name: to_numpy_dtype(dtype)
                    for name, dtype in zip(
                        source.schema.field_names, source.schema.field_types
                    )
                },
            )
            return LocalTable(
                df=df,
                timestamp_field=source.timestamp_field,
                timestamp_format=source.timestamp_format,
            )

        raise RuntimeError(f"Unsupported file format: {source.data_format}.")

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
        dependent_features = []

        for feature in feature_view.get_resolved_features():
            for input_feature in feature.input_features:
                if input_feature not in dependent_features:
                    dependent_features.append(input_feature)
            if feature not in dependent_features:
                dependent_features.append(feature)

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
            source_df[feature.name] = source_df[feature.name].astype(
                to_numpy_dtype(feature.dtype)
            )

        output_fields = feature_view.get_output_fields(source_fields)

        return LocalTable(
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
            features = self.registry.get_features(name=features.name)

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
        join_feature = join_descriptor.get_feature(join_transform.feature_name)
        if join_feature.keys is None:
            raise FeathubException(
                f"The Feature {join_feature} to join must have keys."
            )

        result = []
        # TODO: optimize the performance for the following code.
        for source_idx, source_row in source_df.iterrows():
            source_timestamp = utils.to_unix_timestamp(
                source_row[source_timestamp_field], source_timestamp_format
            )
            joined_value = None
            joined_timestamp = None
            for join_idx, join_row in join_df.iterrows():
                join_timestamp = utils.to_unix_timestamp(
                    join_row[join_timestamp_field], join_timestamp_format
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
                joined_value = join_row[join_transform.feature_name]
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
        agg_func = LocalProcessor._AGG_FUNCTIONS.get(transform.agg_func, None)
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
        unix_time_column = utils.append_unix_time_column(
            df_copy, timestamp_field, timestamp_format
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
            max_timestamp = row[unix_time_column]
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
            predicate = rows_in_group[unix_time_column].transform(
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
                    by=[unix_time_column],
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

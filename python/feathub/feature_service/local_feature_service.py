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
from typing import Optional, List, Dict, Union

from feathub.common.exceptions import FeathubException
from feathub.feature_service.feature_service import FeatureService
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sources.redis_source import RedisSource
from feathub.online_stores.online_store_client import OnlineStoreClient
from feathub.processors.local.ast_evaluator.local_ast_evaluator import LocalAstEvaluator
from feathub.registries.registry import Registry
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_tables.sources.memory_store_source import MemoryStoreSource
from feathub.feature_views.feature import Feature
from feathub.dsl.expr_parser import ExprParser
from feathub.online_stores.memory_online_store import MemoryOnlineStore


class LocalFeatureService(FeatureService):
    """
    A feature service that uses resources on the local machine.
    """

    SERVICE_TYPE = "local"

    def __init__(self, props: Dict, registry: Registry):
        super().__init__()
        self.props = props
        self.registry = registry
        self.parser = ExprParser()
        self.ast_evaluator = LocalAstEvaluator()
        self.online_store_clients: Dict[str, OnlineStoreClient] = {}

    def get_online_features(
        self,
        request_df: pd.DataFrame,
        feature_view: Union[str, OnDemandFeatureView],
        feature_names: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame obtained by applying the given OnDemandFeatureView on the
        given input_data.

        :param request_df: A DataFrame where each row contains the keys of this table.
        :param feature_view: Describes the features to be included in the output. If it
                             is a string, it refers to the name of a OnDemandFeatureView
                             in the entity registry.
        :param feature_names: Optional. The names of fields of values that should be
                               included in the output DataFrame. If it is None, all
                               fields of the specified table should be outputted.
        :return: A DataFrame obtained according to the specified criteria.
        """
        if isinstance(feature_view, str):
            feature_view = self._get_on_demand_feature_view_from_registry(feature_view)
        elif feature_view.is_unresolved():
            feature_view = self._get_on_demand_feature_view_from_registry(
                feature_view.name
            )

        input_fields = request_df.columns.tolist()
        for feature in feature_view.get_resolved_features():
            if isinstance(feature.transform, JoinTransform):
                request_df = self._evaluate_join_transform(request_df, feature)
            elif isinstance(feature.transform, ExpressionTransform):
                request_df = self._evaluate_expression_transform(request_df, feature)
            else:
                raise RuntimeError(
                    f"Unsupported transformation type for feature {feature.to_json()}."
                )

        if feature_names is not None:
            output_fields = feature_names
        else:
            output_fields = feature_view.get_output_fields(input_fields)

        return request_df[output_fields]

    def _get_on_demand_feature_view_from_registry(
        self, feature_view_name: str
    ) -> OnDemandFeatureView:
        feature_view = self.registry.get_features(name=feature_view_name)
        if not isinstance(feature_view, OnDemandFeatureView):
            raise FeathubException(
                f"Expect {feature_view_name} referring to an OnDemandFeatureView "
                f"but got {type(feature_view_name)}."
            )
        return feature_view

    def _evaluate_expression_transform(
        self, df: pd.DataFrame, feature: Feature
    ) -> pd.DataFrame:
        expression_transform = feature.transform
        if not isinstance(expression_transform, ExpressionTransform):
            raise FeathubException(f"Feature {feature} should use ExpressionTransform.")
        expr_node = self.parser.parse(expression_transform.expr)
        df[feature.name] = df.apply(
            lambda row: self.ast_evaluator.eval(expr_node, row), axis=1
        ).tolist()
        return df

    def _evaluate_join_transform(
        self, input_df: pd.DataFrame, feature: Feature
    ) -> pd.DataFrame:
        join_transform = feature.transform
        if not isinstance(join_transform, JoinTransform):
            raise RuntimeError(f"Feature '{feature.name}' should use JoinTransform.")

        source = self.registry.get_features(join_transform.table_name)

        if isinstance(source, MemoryStoreSource):
            return MemoryOnlineStore.get_instance().get(
                table_name=source.table_name, input_data=input_df
            )

        if isinstance(source, RedisSource):
            redis_client = self._get_online_store_client(source)
            return redis_client.get(input_data=input_df)

        raise RuntimeError(f"Unsupported source {source.to_json()}.")

    def _get_online_store_client(self, source: FeatureTable) -> OnlineStoreClient:
        if source.name not in self.online_store_clients:
            client = OnlineStoreClient.instantiate(source)
            self.online_store_clients[source.name] = client

        return self.online_store_clients[source.name]

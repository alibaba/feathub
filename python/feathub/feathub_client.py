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

from typing import Union, Optional, Dict, List
import pandas as pd
from datetime import datetime, timedelta

from feathub.common.config import flatten_dict
from feathub.feature_tables.feature_table import FeatureTable
from feathub.processors.processor import Processor
from feathub.registries.registry import Registry
from feathub.feature_service.feature_service import FeatureService
from feathub.table.table import Table
from feathub.processors.processor_job import ProcessorJob
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView
from feathub.table.table_descriptor import TableDescriptor


class FeathubClient:
    """
    The Feathub client provides APIs to manage features.
    """

    def __init__(self, props: Dict) -> None:
        """
        :param props: Provides the properties to initialize the client.
        """
        self.props = flatten_dict(props)
        self.registry = Registry.instantiate(props=self.props)
        self.processor = Processor.instantiate(props=self.props, registry=self.registry)
        self.feature_service = FeatureService.instantiate(
            props=self.props,
            registry=self.registry,
        )

    # TODO: check whether the start_datetime and end_datetime should be strict.
    def get_features(
        self,
        features: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> Table:
        """
        Returns a table of features according to the specified criteria.

        :param features: Describes the features to be included in the table. If it is a
                         string, it refers to the name of a table descriptor in the
                         entity registry.
        :param keys: Optional. If it is TableDescriptor or DataFrame, it should be
                     transformed into a table of keys. If it is not None, the output
                     table should only include features whose key fields match a row of
                     the table transformed from the `keys`.
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
        :return: A table of features.
        """
        return self.processor.get_table(
            features=features,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def materialize_features(
        self,
        features: Union[str, TableDescriptor],
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ) -> ProcessorJob:
        """
        Starts a job to write a table of features into the given sink according to the
        specified criteria.

        :param features: Describes the table of features to be inserted in the sink. If
                         it is a string, it refers to the name of a table descriptor in
                         the entity registry.
        :param sink: Describes the location to write the features.
        :param ttl: Optional. If it is not None, the features data should be purged from
                    the sink after the specified period of time.
        :param start_datetime: Optional. If it is not None, the `features` table should
                               have a timestamp field. And only writes into sink those
                               features whose timestamp >= floor(start_datetime).
        :param end_datetime: Optional. If it is not None, the `features` table should
                             have a timestamp field. And only writes into sink those
                             features whose timestamp <= ceil(start_datetime).
        :param allow_overwrite: If it is false, throw error if the features collide with
                                existing data in the given sink.
        :return: A processor job corresponding to this materialization operation.
        """
        return self.processor.materialize_features(
            features=features,
            sink=sink,
            ttl=ttl,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            allow_overwrite=allow_overwrite,
        )

    def get_online_features(
        self,
        request_df: pd.DataFrame,
        feature_view: Union[str, OnDemandFeatureView],
        feature_names: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Queries features for the given keys from the online store.

        :return: A DataFrame consisting of the input_data and the requested
                 feature_names.
        """
        return self.feature_service.get_online_features(
            request_df=request_df,
            feature_view=feature_view,
            feature_names=feature_names,
        )

    def build_features(
        self, features_list: List[TableDescriptor], props: Optional[Dict] = None
    ) -> List[TableDescriptor]:
        """
        For each table descriptor in the given list, resolve this descriptor by
        recursively replacing its dependent table and feature names with the
        corresponding table descriptors and features from the cache or registry.
        Then recursively configure the table descriptor and its dependent table that is
        referred by a TableDescriptor with the given global properties if it is not
        configured already.

        And caches the resolved table descriptors in memory so that they can be used
        when building other table descriptors.

        :param features_list: A list of table descriptors.
        :param props: Optional. If it is not None, it is the global properties that are
                      used to configure the given table descriptors.
        :return: A list of resolved descriptors corresponding to the input descriptors.
        """
        return self.registry.build_features(features_list=features_list, props=props)

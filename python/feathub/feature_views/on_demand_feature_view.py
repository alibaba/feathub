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

from typing import Union, Dict, Sequence

from feathub.common import types
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.table.table_descriptor import TableDescriptor
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_tables.sources.online_store_source import OnlineStoreSource
from feathub.registries.registry import Registry
from feathub.feature_views.feature import Feature


class OnDemandFeatureView(FeatureView):
    """
    Derives features by joining online request with features from tables in online
    feature stores. Unlike JoinedFeatureView, transformation of an OnDemandFeatureView
    can only be performed after online request arrives. And it can derive new features
    from the features in user's request.

    Supports per-row transformation and join with tables in online stores. Does not
    support window aggregation transformation. Online request should be provided when
    users request features from this feature view.
    """

    class _OnlineRequestSource(FeatureTable):
        def __init__(self) -> None:
            super().__init__(
                name="_ONLINE_REQUEST",
                system_name="online_request",
                properties={},
                keys=None,
                timestamp_field=None,
                timestamp_format="epoch",
            )

        def to_json(self) -> Dict:
            return {"type": "_OnlineRequestSource"}

    _ONLINE_REQUEST_SOURCE = _OnlineRequestSource()

    def __init__(
        self,
        name: str,
        features: Sequence[Union[str, Feature]],
        keep_source_fields: bool = False,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param features: A list of features to be joined onto this feature view.
                         If a feature is a string, it should be either in the format
                         {table_name}.{feature_name}, which refers to a feature in the
                         table with the given name, or in the format {feature_name},
                         which refers to a feature in the source table.
        :param keep_source_fields: True iff all fields in the source table should be
                                   included in this table.
        """
        super().__init__(
            name=name,
            source=OnDemandFeatureView._ONLINE_REQUEST_SOURCE,
            features=features,
            keep_source_fields=keep_source_fields,
        )
        for feature in features:
            if isinstance(feature, str) and len(feature.split(".")) != 2:
                raise RuntimeError(
                    f"Feature '{feature}' is not in the format "
                    "{table_name}.{feature_name} or {feature_name}."
                )
            if not (
                isinstance(feature, str)
                or isinstance(feature.transform, JoinTransform)
                or isinstance(feature.transform, ExpressionTransform)
            ):
                raise RuntimeError(
                    f"Feature '{feature.name}' uses unsupported transform type "
                    f"'{type(feature.transform)}'."
                )

    # TODO: cache the feature view itself in the registry. Same for other views.
    def build(self, registry: Registry) -> TableDescriptor:
        """
        Gets a copy of self as a resolved table descriptor.

        The features might be strings that reference field names in other tables.
        These references are replaced with the corresponding features.
        """
        features = []
        for feature in self.features:
            if isinstance(feature, str):
                parts = feature.split(".")
                join_table_name = parts[0]
                join_feature_name = parts[1]

                table_desc = registry.get_features(name=join_table_name)

                if not isinstance(table_desc, OnlineStoreSource):
                    raise RuntimeError(f"Unsupported {table_desc.to_json()}.")

                # TODO: consider specifying dtype.
                feature = Feature(
                    name=join_feature_name,
                    dtype=types.Unknown,
                    transform=JoinTransform(join_table_name, join_feature_name),
                    keys=table_desc.keys,
                )

            features.append(feature)

        return OnDemandFeatureView(
            name=self.name,
            features=features,
            keep_source_fields=self.keep_source_fields,
        )

    def is_unresolved(self) -> bool:
        return any(isinstance(f, str) for f in self.features)

    def to_json(self) -> Dict:
        return {
            "type": "OnDemandFeatureView",
            "name": self.name,
            "features": [
                feature if isinstance(feature, str) else feature.to_json()
                for feature in self.features
            ],
            "keep_source_fields": self.keep_source_fields,
        }

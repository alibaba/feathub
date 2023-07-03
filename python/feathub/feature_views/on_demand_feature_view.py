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

from typing import Union, Dict, Sequence, Optional

from feathub.common.utils import from_json, append_metadata_to_json
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sources.memory_store_source import MemoryStoreSource
from feathub.feature_tables.sources.mysql_source import MySQLSource
from feathub.feature_tables.sources.redis_source import RedisSource
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.registries.registry import Registry
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


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
        def __init__(self, schema: Schema) -> None:
            super().__init__(
                name="_ONLINE_REQUEST",
                system_name="online_request",
                table_uri={},
                keys=None,
                timestamp_field=None,
                timestamp_format="epoch",
                schema=schema,
            )

        def to_json(self) -> Dict:
            pass

    def __init__(
        self,
        name: str,
        features: Sequence[Union[str, Feature]],
        request_schema: Schema,
        keep_source_fields: bool = False,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param features: A list of features to be computed in this feature view. If a
                         feature is a string, it should be in either of the following
                         formats:
                         1. {feature_name}, which refers to a feature in the table with
                            the given name
                         2. {table_name}.{feature_name}, which refers to a feature in
                            the source table
        :param request_schema: The schema of the request expected by the
                               OnDemandFeatureView.
        :param keep_source_fields: True iff all fields in the source table should be
                                   included in this table. The feature in the source
                                   will be overwritten by the feature in this feature
                                   view if they have the same name.
        """
        super().__init__(
            name=name,
            source=OnDemandFeatureView._OnlineRequestSource(schema=request_schema),
            features=features,
            keep_source_fields=keep_source_fields,
        )
        self.request_schema = request_schema
        for feature in features:
            if isinstance(feature, str) and len(feature.split(".")) != 2:
                raise RuntimeError(
                    f"Feature '{feature}' is not in the format "
                    "{table_name}.{feature_name}."
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

    def build(
        self,
        registry: "Registry",
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> TableDescriptor:
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

                table_desc = registry.get_features(
                    name=join_table_name, force_update=force_update
                )

                if (
                    isinstance(table_desc, MemoryStoreSource)
                    or isinstance(table_desc, RedisSource)
                    or isinstance(table_desc, MySQLSource)
                ):
                    feature = Feature(
                        name=join_feature_name,
                        dtype=table_desc.get_feature(join_feature_name).dtype,
                        transform=JoinTransform(
                            join_table_name, f"`{join_feature_name}`"
                        ),
                        keys=table_desc.keys,
                    )

                else:
                    raise RuntimeError(f"Unsupported {table_desc.to_json()}.")

            features.append(feature)

        return OnDemandFeatureView(
            name=self.name,
            features=features,
            request_schema=self.request_schema,
            keep_source_fields=self.keep_source_fields,
        )

    def is_unresolved(self) -> bool:
        return any(isinstance(f, str) for f in self.features)

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "features": [
                feature if isinstance(feature, str) else feature.to_json()
                for feature in self.features
            ],
            "request_schema": self.request_schema.to_json(),
            "keep_source_fields": self.keep_source_fields,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "OnDemandFeatureView":
        return OnDemandFeatureView(
            name=json_dict["name"],
            features=[
                feature if isinstance(feature, str) else from_json(feature)
                for feature in json_dict["features"]
            ],
            request_schema=from_json(json_dict["request_schema"]),
            keep_source_fields=json_dict["keep_source_fields"],
        )

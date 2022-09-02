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
from abc import ABC
from typing import List, Optional

from feathub.common.exceptions import FeathubException
from feathub.feature_views.feature import Feature
from feathub.table.table_descriptor import TableDescriptor
from feathub.table.schema import Schema


# TODO: add SQL source which outputs the table returned by a SQL query.
class Source(TableDescriptor, ABC):
    """
    Provides metadata to access and interpret a table of feature values from an
    offline or online feature store.
    """

    def __init__(
        self,
        name: str,
        keys: Optional[List[str]],
        timestamp_field: Optional[str],
        timestamp_format: str,
        schema: Optional[Schema] = None,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field.
        :param schema: Optional. If schema is not None. The source can automatically
                       derive feature for each field in the schema.

        """
        super().__init__(
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.schema = schema

    def get_feature(self, feature_name: str) -> Feature:
        if self.schema is None:
            raise FeathubException(
                "The source does not have schema. Feature can not be derived. You "
                "should create a FeatureView with this source and define the features"
                "explicitly in the FeatureView."
            )

        if feature_name not in self.schema.field_names:
            raise FeathubException(
                f"Failed to find the feature '{feature_name}' in {self.to_json()}."
            )

        return Feature(
            name=feature_name,
            dtype=self.schema.get_field_type(feature_name),
            transform=feature_name,
            keys=self.keys,
        )

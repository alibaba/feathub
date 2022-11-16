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
from abc import ABC
from typing import List, Optional, Dict, Any

from feathub.common.exceptions import FeathubException
from feathub.feature_views.feature import Feature
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


# TODO: Update Sink implementation of FeatureTable to support rename timestamp field
#  and convert timestamp_format.
class FeatureTable(TableDescriptor, ABC):
    """
    Provides properties to uniquely identify and describe a physical table.
    """

    def __init__(
        self,
        name: str,
        system_name: str,
        properties: Dict[str, Any],
        data_format: Optional[str] = None,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        schema: Optional[Schema] = None,
    ):
        """
        :param name: The name that uniquely identifies this feature table in a registry.
        :param system_name: Uniquely identifies the underlying system, e.g. filesystem,
                            kafka, etc.
        :param properties: It contains the properties specific to the underlying system
                           that are used to uniquely identify the physical table.
        :param data_format: Optional. If it is not None, it specifies the format of the
                            data, e.g. csv, json, parquet, etc. This is typically
                            used by storage that does not require schema, e.g.
                            filesystem, kafka, etc.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field.
        :param schema: Optional. If schema is not None, the feature table automatically
                       derives feature for each field in the schema when reading from
                       the physical table. If the schema of is None, it uses the same
                       schema of its upstream table when writing to the physical table.
                       Otherwise, the subclass should overwrite the get_feature method
                       to derive the feature according to the schema of the underlying
                       system.
        """
        super().__init__(
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.data_format = data_format
        self.system_name = system_name
        self.properties = properties
        self.schema = schema

    def get_feature(self, feature_name: str) -> Feature:
        if self.schema is None:
            raise FeathubException(
                "The feature table does not have schema. Feature can not be derived. "
                "You should create a FeatureView that uses this feature table as source"
                "and define the features explicitly in the FeatureView."
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

    def is_bounded(self) -> bool:
        # FeatureTable is bounded by default unless subclass overwrite the method.
        return True

    def get_bounded_view(self) -> TableDescriptor:
        if self.is_bounded():
            return self

        raise FeathubException(
            f"{type(self)} is unbounded and it doesn't support getting bounded view."
        )

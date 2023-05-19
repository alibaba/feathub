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
from typing import Dict, Sequence, Optional, List

from feathub.common.exceptions import FeathubException
from feathub.common.utils import from_json, append_metadata_to_json
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.registries.registry import Registry
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class SqlFeatureView(FeatureView):
    """
    Derives features by evaluating a given SQL statement.

    Currently, the SQL statement is executed by the underlying processor's SQL interface
    without any change and therefore its semantics depends on the processor used during
    deployment.

    While executing the same SQL statement on different processors usually yields
    identical results, there is no guarantee of this. We plan to make SqlFeatureView
    processor-agnostic in the future to ensure consistent semantics regardless of
    processor choice.

    NOTE: the tables used in the SQL statement should be built with the
    FeatHubClient#build_features() before executing the SqlFeatureView.
    """

    def __init__(
        self,
        name: str,
        sql_statement: str,
        schema: Schema,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        is_bounded: bool = True,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param sql_statement: The SQL query statement to be evaluated with an internal
                              processor.
        :param schema: The schema of the output table.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field. See TableDescriptor
                                 for valid format values. Only effective when the
                                 `timestamp_field` is not None.
        :param is_bounded: Whether the output table of this SqlFeatureView is bounded.
        """
        TableDescriptor.__init__(
            self,
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.sql_statement = sql_statement
        self.schema = schema

        if timestamp_field is not None and timestamp_field not in schema.field_names:
            raise FeathubException(
                f"Schema {schema} does not contain timestamp field {timestamp_field}."
            )

        if keys is not None and any(key not in schema.field_names for key in keys):
            raise FeathubException(f"Schema {schema} does not contain all keys {keys}.")

        self._is_bounded = is_bounded

    def build(
        self,
        registry: "Registry",
        force_update: bool = False,
        props: Optional[Dict] = None,
    ) -> TableDescriptor:
        return SqlFeatureView(
            name=self.name,
            sql_statement=self.sql_statement,
            schema=self.schema,
            keys=self.keys,
            timestamp_field=self.timestamp_field,
            timestamp_format=self.timestamp_format,
            is_bounded=self._is_bounded,
        )

    def is_unresolved(self) -> bool:
        return False

    def get_output_fields(self, source_fields: List[str]) -> List[str]:
        return self.schema.field_names.copy()

    def get_output_features(self) -> List[Feature]:
        return [self.get_feature(field_name) for field_name in self.schema.field_names]

    def get_feature(self, feature_name: str) -> Feature:
        return Feature(
            name=feature_name,
            dtype=self.schema.get_field_type(feature_name),
            transform=feature_name,
            keys=self.keys,
        )

    def get_resolved_features(self) -> Sequence[Feature]:
        return self.get_output_features()

    def get_resolved_source(self) -> TableDescriptor:
        raise FeathubException("Unsupported Operation.")

    def is_bounded(self) -> bool:
        return self._is_bounded

    def get_bounded_view(self) -> TableDescriptor:
        if not self._is_bounded:
            raise FeathubException(
                "SqlFeatureView is unbounded and it doesn't support getting bounded "
                "view."
            )
        return self

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "sql_statement": self.sql_statement,
            "schema": self.schema.to_json(),
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "is_bounded": self._is_bounded,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "SqlFeatureView":
        return SqlFeatureView(
            name=json_dict["name"],
            sql_statement=json_dict["sql_statement"],
            schema=from_json(json_dict["schema"]),
            keys=json_dict["keys"],
            timestamp_field=json_dict["timestamp_field"],
            timestamp_format=json_dict["timestamp_format"],
            is_bounded=json_dict["is_bounded"],
        )

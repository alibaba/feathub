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

from typing import List, Optional

from feathub.table.table import TableDescriptor
from feathub.feature_views.feature import Feature


# TODO: add SQL source which outputs the table returned by a SQL query.
class Source(TableDescriptor):
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
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :timestamp_format: The format of the timestamp field.
        """
        super().__init__(
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    def get_feature(self, feature_name: str) -> Feature:
        raise RuntimeError(
            f"Failed to find the feature '{feature_name}' in {self.to_json()}."
        )

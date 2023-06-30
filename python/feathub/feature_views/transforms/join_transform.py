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
from typing import Dict

from feathub.common.utils import append_metadata_to_json, deprecated_alias
from feathub.feature_views.transforms.transformation import Transformation


class JoinTransform(Transformation):
    """
    Derives feature values by joining parent table with a feature from another table.
    """

    @deprecated_alias(feature_name="expr")
    def __init__(
        self,
        table_name: str,
        expr: str,
    ):
        """
        :param table_name: The name of a Source or FeatureView table.
        :param expr: The feature expr. it should be in either of the following
                     formats:
                     1. {feature_name}, which refers to a feature in the host table
                     2. {map_feature_name}[{literal_key_value}], which refers to a
                        static lookup of a map feature in the host table with the
                        given name
        """
        super().__init__()
        self.table_name = table_name
        self.expr = expr

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "table_name": self.table_name,
            "expr": self.expr,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "JoinTransform":
        return JoinTransform(
            table_name=json_dict["table_name"],
            expr=json_dict["expr"],
        )

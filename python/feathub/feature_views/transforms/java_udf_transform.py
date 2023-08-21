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
from typing import Dict, Sequence, Any

from feathub.common.utils import append_metadata_to_json, from_json
from feathub.feature_views.transforms.transformation import Transformation
from feathub.table.schema import Schema


# TODO: Support watermark for output tables of this transform.
class JavaUdfTransform(Transformation):
    """
    Derives feature values by applying a Java UDF on one row of the parent table at a
    time. The UDF would be responsible for determining the whole output of the host
    feature view.

    For FlinkProcessor, this UDF should be an instance of
    org.apache.flink.streaming.api.operators.OneInputStreamOperator.

    WARN: this is an internal class and there is no backward compatibility guarantee for
    its API. FeatHub users should not use this class directly.
    """

    def __init__(
        self, class_name: str, parameters: Sequence[Any], schema: Schema
    ) -> None:
        """
        :param class_name: The java class name of the UDF.
        :param parameters: The constructor parameters of the java class. A parameter
                           value should be of primitive type and not be null.
        :param schema: The schema of the output table.
        """
        super().__init__()
        self.class_name = class_name
        self.parameters = parameters
        self.schema = schema

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "class_name": self.class_name,
            "parameters": self.parameters,
            "schema": self.schema.to_json(),
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "JavaUdfTransform":
        return JavaUdfTransform(
            class_name=json_dict["class_name"],
            parameters=json_dict["parameters"],
            schema=from_json(json_dict["schema"]),
        )

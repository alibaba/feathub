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

from abc import ABC, abstractmethod
import json
from typing import Dict


class Entity(ABC):
    """An entity is the primary node to make up a metadata graph.

    An entity can refer to e.g. a dataset or a user. The concept is similar to
    the entity defined in the DataHub metadata model
    (https://github.com/datahub-project/datahub/blob/master/docs/modeling/metadata-model.md).
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        """
        Returns a json-formatted object representing this entity.
        """
        pass

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and self.to_json() == other.to_json()

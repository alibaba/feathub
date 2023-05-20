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

import json
from abc import ABC, abstractmethod
from typing import Dict


class Transformation(ABC):
    """
    Abstract class for classes that define how to derive a new feature from existing
    features.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        """
        Returns a json-formatted object representing this sink.
        """
        pass

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)

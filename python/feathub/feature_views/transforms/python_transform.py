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

from typing import Callable, Any, Dict
import pandas as pd

from feathub.feature_views.transforms.transformation import Transformation


# TODO: support python udf.
class PythonUdfTransform(Transformation):
    """
    Derives feature values by applying a Python UDF on one row of the parent table at a
    time.
    """

    def __init__(self, callable: Callable[[pd.DataFrame], Any]) -> None:
        super().__init__()
        self.callable = callable

    def to_json(self) -> Dict:
        return {"type": "PythonTransform"}

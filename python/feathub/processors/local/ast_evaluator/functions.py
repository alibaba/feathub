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
from typing import Callable, Dict, Optional

import feathub.common.utils as utils

# TODO: add Flink's System (Built-in) Functions
_FUNCTIONS: Dict[str, Callable] = {
    "unix_timestamp": utils.to_unix_timestamp,
}


def get_predefined_function(name: str) -> Optional[Callable]:
    return _FUNCTIONS.get(name.lower(), None)

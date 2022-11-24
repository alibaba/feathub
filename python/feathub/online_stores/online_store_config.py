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

from enum import Enum
from typing import Dict, Any

from feathub.common.config import ConfigDef, BaseConfig
from feathub.common.validators import is_subset


class OnlineStoreType(Enum):
    MEMORY = "memory"


ONLINE_STORE_TYPES_CONFIG = "online_store.types"
ONLINE_STORE_TYPES_DOC = "A list of online stores to use."


class OnlineStoreConfig(BaseConfig):
    def __init__(self, props: Dict[str, Any]):
        super().__init__(
            [
                ConfigDef(
                    name=ONLINE_STORE_TYPES_CONFIG,
                    value_type=list,
                    description=ONLINE_STORE_TYPES_DOC,
                    default_value=[],
                    validator=is_subset(*[t.value for t in OnlineStoreType]),
                )
            ],
            props,
        )

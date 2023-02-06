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
from datetime import tzinfo, timezone
from typing import Any

from feathub.common.utils import to_unix_timestamp


class LocalFuncEvaluator:
    def __init__(self, tz: tzinfo = timezone.utc):
        self.tz = tz

    def eval(self, func_name: str, values: Any) -> Any:
        if func_name == "LOWER":
            return values[0].lower()
        elif func_name == "UNIX_TIMESTAMP":
            if len(values) == 1:
                return to_unix_timestamp(values[0], tz=self.tz)
            else:
                return to_unix_timestamp(values[0], values[1], self.tz)
        raise RuntimeError(f"Unsupported function: {func_name}.")

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
from datetime import tzinfo, timezone
from typing import Any

from feathub.common.exceptions import FeathubException
from feathub.common.utils import to_unix_timestamp


class LocalFuncEvaluator:
    def __init__(self, tz: tzinfo = timezone.utc):
        self.tz = tz

    def eval(self, func_name: str, values: Any) -> Any:
        if func_name == "LOWER":
            return values[0].lower()
        elif func_name == "CONCAT":
            return "".join([str(x) for x in values])
        elif func_name == "CONCAT_WS":
            return values[0].join([str(x) for x in values[1:]])
        elif func_name == "UNIX_TIMESTAMP":
            if values[0] is None:
                return None
            if len(values) == 1:
                return int(to_unix_timestamp(values[0], tz=self.tz))
            else:
                return int(to_unix_timestamp(values[0], values[1], self.tz))
        elif func_name == "JSON_STRING":
            if values[0] is None:
                return None
            return json.dumps(values[0], separators=(",", ":"))
        elif func_name == "MAP":
            if len(values) % 2 != 0:
                raise FeathubException("Map requires an even number of arguments.")
            res = {}
            for i in range(0, len(values), 2):
                res[values[i]] = values[i + 1]
            return res
        elif func_name == "SIZE":
            if values[0] is None:
                return None
            return len(values[0])
        raise RuntimeError(f"Unsupported function: {func_name}.")

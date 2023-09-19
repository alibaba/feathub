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
from typing import List, Any

from feathub.common.utils import to_java_date_format


"""
This set contains the name of the built-in functions whose name
and argument list is the same between FeatHub and in Flink.
"""
_functions_with_equal_signature = {"LOWER", "JSON_STRING"}


def evaluate_function(func_name: str, args: List[Any]) -> str:
    if func_name in _functions_with_equal_signature:
        return f"{func_name}({', '.join(args)})"
    elif func_name in {"CONCAT", "CONCAT_WS"}:
        tmp_args = args.copy()
        for i in range(len(tmp_args)):
            if i == 0 and func_name == "CONCAT_WS":
                continue
            tmp_args[i] = f"CAST({tmp_args[i]} AS STRING)"
        return f"{func_name}({', '.join(tmp_args)})"
    elif func_name == "UNIX_TIMESTAMP":
        if len(args) > 1:
            args[1] = to_java_date_format(args[1])
        return f"{func_name}({', '.join(args)})"
    elif func_name == "MAP":
        return f"MAP[{', '.join(args)}]"
    elif func_name == "ARRAY":
        return f"ARRAY[{', '.join(args)}]"
    elif func_name == "SIZE":
        return f"CARDINALITY({', '.join(args)})"
    raise RuntimeError(f"Unsupported function: {func_name}.")

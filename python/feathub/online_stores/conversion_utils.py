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
from datetime import datetime
from typing import List, Dict, Any, Union

from feathub.common import types
from feathub.common.exceptions import FeathubException


def to_python_object(
    serialized_data: Union[bytes, Dict[bytes, bytes], List[bytes]],
    data_type: types.DType,
) -> Any:
    """
    Converts a feature from serialized bytes to a Python object according to
    the data type.
    """
    if isinstance(data_type, types.VectorType):
        if isinstance(serialized_data, bytes):
            list_data = [x.encode("utf-8") for x in json.loads(serialized_data)]
        elif isinstance(serialized_data, List):
            list_data = serialized_data
        else:
            raise FeathubException(
                f"Cannot parse {type(serialized_data)} data according to "
                f"type {data_type}."
            )
        return [to_python_object(x, data_type.dtype) for x in list_data]

    if isinstance(data_type, types.MapType):
        if isinstance(serialized_data, bytes):
            map_data = {
                k.encode("utf-8"): v.encode("utf-8")
                for k, v in json.loads(serialized_data).items()
            }
        elif isinstance(serialized_data, Dict):
            map_data = serialized_data
        else:
            raise FeathubException(
                f"Cannot parse {type(serialized_data)} data according to "
                f"type {data_type}."
            )
        return {
            to_python_object(key, data_type.key_dtype): to_python_object(
                value, data_type.value_dtype
            )
            for key, value in map_data.items()
        }

    if not isinstance(serialized_data, bytes):
        raise FeathubException(
            f"Cannot parse {type(serialized_data)} data according to "
            f"type {data_type}."
        )

    if data_type == types.Bytes:
        return serialized_data

    data = serialized_data.decode("utf-8")

    if data_type == types.String:
        return data
    elif data_type == types.Bool:
        return data == "true"
    elif data_type == types.Int32:
        return int(data)
    elif data_type == types.Int64:
        return int(data)
    elif data_type == types.Float32:
        return float(data)
    elif data_type == types.Float64:
        return float(data)
    elif data_type == types.Timestamp:
        return datetime.fromtimestamp(int(data) / 1000.0)

    raise FeathubException(
        f"Cannot parse {type(serialized_data)} data according to type {data_type}."
    )

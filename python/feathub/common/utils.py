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

from datetime import datetime, timezone, tzinfo
from string import Template
from typing import Union, Any, Optional, cast

import pandas as pd

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.common.protobuf import value_pb2


def to_java_date_format(python_format: str) -> str:
    """
    :param python_format: A datetime format string accepted by datetime::strptime().
    :return: A datetime format string accepted by java.text.SimpleDateFormat.
    """

    # TODO: Currently cannot handle case such as "%Y-%m-%dT%H:%M:%S", which should be
    #  converted to "yyyy-MM-dd'T'HH:mm:ss".
    mapping = {
        "Y": "yyyy",
        "m": "MM",
        "d": "dd",
        "H": "HH",
        "M": "mm",
        "S": "ss",
        "f": "SSS",
        "z": "X",
    }
    return Template(python_format.replace("%", "$")).substitute(**mapping)


def to_unix_timestamp(
    time: Union[int, datetime, str],
    format: str = "%Y-%m-%d %H:%M:%S",
    tz: tzinfo = timezone.utc,
) -> float:
    """
    Returns POSIX timestamp corresponding to date_string, parsed according to format.
    Uses the timezone specified in tz if it is not explicitly specified in the given
    date.
    """
    if isinstance(time, str):
        time = datetime.strptime(time, format)
    elif isinstance(time, int):
        if format == "epoch":
            time = datetime.fromtimestamp(time, tz=tz)
        elif format == "epoch_millis":
            time = datetime.fromtimestamp(time / 1000, tz=tz)
        else:
            raise FeathubException(
                f"Unknown type {type(time)} of timestamp with timestamp "
                f"format {format}."
            )
    if time.tzinfo is None:
        time = time.replace(tzinfo=tz)
    return time.timestamp()


def append_unix_time_column(
    df: pd.DataFrame, timestamp_field: str, timestamp_format: str
) -> str:
    unix_time_column = "_unix_time"

    if unix_time_column in df:
        raise RuntimeError(f"The dataframe has column with name {unix_time_column}.")

    df[unix_time_column] = df.apply(
        lambda row: to_unix_timestamp(row[timestamp_field], timestamp_format),
        axis=1,
    )
    return unix_time_column


def append_and_sort_unix_time_column(
    df: pd.DataFrame, timestamp_field: str, timestamp_format: str
) -> str:
    unix_time_column = append_unix_time_column(df, timestamp_field, timestamp_format)

    df.sort_values(
        by=[unix_time_column],
        ascending=True,
        inplace=True,
        ignore_index=True,
    )

    return unix_time_column


def serialize_object_with_protobuf(
    feature_object: Optional[Any], feature_type: types.DType
) -> bytes:
    """
    Serializes a feature value into byte array with protobuf.

    :param feature_object: The feature value to be serialized.
    :param feature_type: The type of the feature value.
    """
    return _serialize_object_with_protobuf(
        feature_object, feature_type
    ).SerializeToString()


def _serialize_object_with_protobuf(
    feature_object: Optional[Any], feature_type: types.DType
) -> value_pb2.Value:
    pb_value = value_pb2.Value()
    if feature_object is None:
        pb_value.none_value = True
    elif feature_type == types.Bytes:
        pb_value.bytes_value = feature_object
    elif feature_type == types.String:
        pb_value.string_value = feature_object
    elif feature_type == types.Bool:
        pb_value.boolean_value = feature_object
    elif feature_type == types.Int32:
        pb_value.int_value = feature_object
    elif feature_type == types.Int64:
        pb_value.long_value = feature_object
    elif feature_type == types.Float32:
        pb_value.float_value = feature_object
    elif feature_type == types.Float64:
        pb_value.double_value = feature_object
    elif feature_type == types.Timestamp:
        timestamp_value = cast(datetime, feature_object)
        pb_value.timestamp_value.FromDatetime(timestamp_value)
    elif isinstance(feature_type, types.MapType):
        map_type: types.MapType = cast(types.MapType, feature_type)
        map_object: dict = cast(dict, feature_object)
        for key, value in map_object.items():
            pb_value.map_value.keys.append(
                _serialize_object_with_protobuf(key, map_type.key_dtype)
            )
            pb_value.map_value.values.append(
                _serialize_object_with_protobuf(value, map_type.value_dtype)
            )
    elif isinstance(feature_type, types.VectorType):
        vector_type: types.VectorType = cast(types.VectorType, feature_type)
        vector_object: list = cast(list, feature_object)
        for element in vector_object:
            pb_value.vector_value.values.append(
                _serialize_object_with_protobuf(element, vector_type.dtype)
            )
    else:
        raise TypeError(f"Unsupported data type {feature_type}")
    return pb_value


def deserialize_object_with_protobuf(pb_byte_array: bytes) -> Optional[Any]:
    """
    Deserializes a feature value from byte array with protobuf.

    :param pb_byte_array: The protobuf byte array to be deserialized.
    """
    pb_value = value_pb2.Value()
    pb_value.ParseFromString(pb_byte_array)
    return _deserialize_object_with_protobuf(pb_value)


def _deserialize_object_with_protobuf(pb_value: value_pb2.Value) -> Optional[Any]:
    if pb_value.HasField("none_value"):
        return None
    elif pb_value.HasField("bytes_value"):
        return pb_value.bytes_value
    elif pb_value.HasField("string_value"):
        return pb_value.string_value
    elif pb_value.HasField("boolean_value"):
        return pb_value.boolean_value
    elif pb_value.HasField("int_value"):
        return pb_value.int_value
    elif pb_value.HasField("long_value"):
        return pb_value.long_value
    elif pb_value.HasField("float_value"):
        return pb_value.float_value
    elif pb_value.HasField("double_value"):
        return pb_value.double_value
    elif pb_value.HasField("timestamp_value"):
        return pb_value.timestamp_value.ToDatetime()
    elif pb_value.HasField("map_value"):
        map_object = dict()
        for raw_key, raw_value in zip(
            pb_value.map_value.keys, pb_value.map_value.values
        ):
            key = _deserialize_object_with_protobuf(raw_key)
            value = _deserialize_object_with_protobuf(raw_value)
            map_object[key] = value
        return map_object
    elif pb_value.HasField("vector_value"):
        vector_object = list()
        for value in pb_value.vector_value.values:
            element = _deserialize_object_with_protobuf(value)
            vector_object.append(element)
        return vector_object
    else:
        raise TypeError(f"Cannot deserialize protobuf object {pb_value}")

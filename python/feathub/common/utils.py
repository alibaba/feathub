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
import sys
import warnings
from datetime import datetime, timezone, tzinfo
from string import Template
from typing import Union, Dict, Any, TYPE_CHECKING, Callable
from urllib.parse import urlparse

from feathub.common.exceptions import FeathubException

if TYPE_CHECKING:
    from feathub.table.schema import Schema
    from feathub.table.table_descriptor import TableDescriptor


JSON_FORMAT_VERSION = 1


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


def get_table_schema(table: "TableDescriptor") -> "Schema":
    """
    Return the schema of the table.
    """
    from feathub.table.schema import Schema

    schema_builder = Schema.new_builder()
    for f in table.get_output_features():
        schema_builder.column(f.name, f.dtype)
    return schema_builder.build()


def is_local_file_or_dir(url: str) -> bool:
    """
    Check whether a url represents a local file or directory.
    """
    url_parsed = urlparse(url)
    return url_parsed.scheme in ("file", "")


def append_metadata_to_json(func: Any) -> Any:
    """
    Decorates to_json methods, additionally saving the following
    metadata to each json dict.

    - "class": The full module and class name of the generator class.
    - "version": The version of the used json format. Currently version
                 value can only be 1.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        json_dict = func(*args, **kwargs)
        if "class" in json_dict or "version" in json_dict:
            raise FeathubException(
                f"{func.__module__}.{func.__qualname__} should not contain metadata "
                f"keys."
            )
        json_dict["class"] = func.__module__ + "." + func.__qualname__.split(".")[0]
        json_dict["version"] = JSON_FORMAT_VERSION
        return json_dict

    return wrapper


def from_json(json_dict: Dict) -> Any:
    """
    Converts a json dict to Python object. The input json dict must be generated
    by a to_json method decorated by append_metadata_to_json.
    """

    if json_dict["version"] != JSON_FORMAT_VERSION:
        raise FeathubException(
            f"Unsupported json format version {json_dict['version']}."
        )

    delimiter_index = str(json_dict["class"]).rindex(".")
    module_name = json_dict["class"][:delimiter_index]

    # TODO: Modify configurations to align the format requirement between
    #  black and flake8 about whitespaces between functions and colons.

    # avoid contradict requirements for code format between black and flake8
    class_name_start_index = delimiter_index + 1
    class_name = json_dict["class"][class_name_start_index:]

    return getattr(sys.modules[module_name], class_name).from_json(json_dict)


def deprecated_alias(**aliases: str) -> Callable:
    """
    Decorator for deprecated function and method arguments.

    Use as follows:

    @deprecated_alias(old_arg='new_arg')
    def myfunc(new_arg):
        ...

    """

    def deco(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rename_kwargs(func.__name__, kwargs, aliases)
            return func(*args, **kwargs)

        return wrapper

    return deco


def rename_kwargs(
    func_name: str, kwargs: Dict[str, Any], aliases: Dict[str, str]
) -> None:
    """Helper function for deprecating function arguments."""
    for alias, new in aliases.items():
        if alias in kwargs:
            if new in kwargs:
                raise TypeError(
                    f"{func_name} received both {alias} and {new} as arguments!"
                    f" {alias} is deprecated, use {new} instead."
                )
            warnings.warn(
                message=(
                    f"`{alias}` is deprecated as an argument to `{func_name}`; use"
                    f" `{new}` instead."
                ),
                category=DeprecationWarning,
                stacklevel=3,
            )
            kwargs[new] = kwargs.pop(alias)

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
from datetime import datetime, timezone, tzinfo
from string import Template
from typing import Union
from urllib.parse import urlparse

from feathub.common.exceptions import FeathubException
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


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


def get_table_schema(table: TableDescriptor) -> Schema:
    """
    Return the schema of the table.
    """
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

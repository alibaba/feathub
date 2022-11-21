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

import pandas as pd
from datetime import datetime, timezone
from string import Template
from typing import Union


def to_java_date_format(python_format: str) -> str:
    """
    :param python_format: A datetime format string accepted by datetime::strptime().
    :return: A datetime format string accepted by  java.text.SimpleDateFormat.
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
    time: Union[datetime, str], format: str = "%Y-%m-%d %H:%M:%S"
) -> float:
    """
    Returns POSIX timestamp corresponding to date_string, parsed according to format.
    Uses UTC timezone if it is not explicitly sepcified in the given date.
    """
    if isinstance(time, str):
        time = datetime.strptime(time, format)
    if time.tzinfo is None:
        time = time.replace(tzinfo=timezone.utc)
    return time.timestamp()


def append_and_sort_unix_time_column(
    df: pd.DataFrame, timestamp_field: str, timestamp_format: str
) -> str:
    unix_time_column = "_unix_time"

    if unix_time_column in df:
        raise RuntimeError(f"The dataframe has column with name {unix_time_column}.")

    df[unix_time_column] = df.apply(
        lambda row: to_unix_timestamp(row[timestamp_field], timestamp_format),
        axis=1,
    )
    df.sort_values(
        by=[unix_time_column],
        ascending=True,
        inplace=True,
        ignore_index=True,
    )

    return unix_time_column

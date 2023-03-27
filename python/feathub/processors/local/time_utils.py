#  Copyright 2022 The FeatHub Authors
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
from datetime import tzinfo

import pandas as pd

from feathub.common.utils import to_unix_timestamp
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME


def append_unix_time_column(
    df: pd.DataFrame, timestamp_field: str, timestamp_format: str, tz: tzinfo
) -> None:
    if EVENT_TIME_ATTRIBUTE_NAME in df:
        raise RuntimeError(
            f"The dataframe has column with name {EVENT_TIME_ATTRIBUTE_NAME}."
        )

    df[EVENT_TIME_ATTRIBUTE_NAME] = df.apply(
        lambda row: to_unix_timestamp(row[timestamp_field], timestamp_format, tz),
        axis=1,
    )


def append_and_sort_unix_time_column(
    df: pd.DataFrame, timestamp_field: str, timestamp_format: str, tz: tzinfo
) -> None:
    append_unix_time_column(df, timestamp_field, timestamp_format, tz)

    df.sort_values(
        by=[EVENT_TIME_ATTRIBUTE_NAME],
        ascending=True,
        inplace=True,
        ignore_index=True,
    )

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

from datetime import timedelta


def timedelta_to_flink_sql_interval(
    _timedelta: timedelta, day_precision: int = 5, fractional_precision: int = 3
) -> str:
    """
    Convert the given timedelta to Flink SQL interval string. The returned interval
    consists of +days hours:months:seconds_of_day.fractional.

    :param _timedelta: The timedelta to be converted
    :param day_precision: Number of digits of days
    :param fractional_precision: Number of digits of fractional seconds
    :return: The flink sql interval string representation.
    """
    days = _timedelta.days
    seconds_of_day = _timedelta.seconds
    microsecond = _timedelta.microseconds
    millisecond = microsecond // 1000

    hours = seconds_of_day // 3600
    minutes = (seconds_of_day % 3600) // 60
    seconds = seconds_of_day % 60

    return (
        f"INTERVAL '{days:02d} "
        f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millisecond:03d}' "
        f"DAY({day_precision}) TO SECOND({fractional_precision})"
    )

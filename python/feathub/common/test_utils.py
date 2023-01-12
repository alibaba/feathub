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

from datetime import datetime


def to_epoch_millis(
    timestamp_str: str, timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f"
) -> int:
    """
    Returns the number of milliseconds since epoch for the given timestamp string.
    """
    return int(datetime.strptime(timestamp_str, timestamp_format).timestamp() * 1000)


def to_epoch(timestamp_str: str, timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f") -> int:
    """
    Returns the number of seconds since epoch for the given timestamp string.
    """
    return int(datetime.strptime(timestamp_str, timestamp_format).timestamp())

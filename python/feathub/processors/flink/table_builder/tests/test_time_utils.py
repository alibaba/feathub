#  Copyright 2022 The Feathub Authors
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

import unittest
from datetime import timedelta

from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)


class TimeUtilsTest(unittest.TestCase):
    def test_timedelta_to_flink_sql_interval(self):
        self.assertEqual(
            "INTERVAL '00 00:01:10.000' DAY(5) TO SECOND(3)",
            timedelta_to_flink_sql_interval(timedelta(seconds=70)),
        )

        self.assertEqual(
            "INTERVAL '02 01:00:00.100' DAY(5) TO SECOND(3)",
            timedelta_to_flink_sql_interval(
                timedelta(seconds=3600, milliseconds=100, days=2)
            ),
        )

        self.assertEqual(
            "INTERVAL '02 01:01:01.100' DAY(5) TO SECOND(3)",
            timedelta_to_flink_sql_interval(
                timedelta(days=2, hours=1, minutes=1, seconds=1, milliseconds=100)
            ),
        )

        self.assertEqual(
            "INTERVAL '99999 23:59:59.999' DAY(5) TO SECOND(3)",
            timedelta_to_flink_sql_interval(
                timedelta(
                    days=99999, hours=23, minutes=59, seconds=59, milliseconds=999
                )
            ),
        )

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
import json
from datetime import timedelta
from typing import Dict, Optional, List, Callable, Any

import pandas as pd
from pyflink.table import ScalarFunction, AggregateFunction
from pyflink.table.udf import ACC, T


class JsonStringToMap(ScalarFunction):
    """A scalar function that map a json string to map."""

    def eval(self, s: str) -> Dict:
        return json.loads(s)


# TODO: Implement in Java for better performance.
class ValueCountsJsonWithRowLimit(AggregateFunction):
    """
    An aggregate function that counts each unique value and returns as json string.
    """

    def __init__(self, limit: Optional[int]) -> None:
        """
        :param limit: Optional. If it is not none, the aggregate function only includes
                      the latest `limit` rows.
        """
        self.limit = limit

    def get_value(self, accumulator: List[pd.Series]) -> str:
        return accumulator[0].to_json()

    def create_accumulator(self) -> List[pd.Series]:
        return []

    def accumulate(self, accumulator: List[pd.Series], *args: pd.Series) -> None:
        df = pd.DataFrame({"val": args[0], "time": args[1]})
        if self.limit is not None:
            df = df.nlargest(self.limit, ["time"], keep="all").iloc[: self.limit]
        accumulator.append(df["val"].value_counts())


# TODO: We can implement the TimeWindowedAggFunction in Java to get better performance.
class TimeWindowedAggFunction(AggregateFunction):
    """
    An aggregate function for Flink table aggregation.

    The aggregate function only aggregate rows with row time in the range of
    [current_row_time - time_interval, current_row_time]. Currently, Flink SQL/Table
    only support over window with either time range or row count-based range. This can
    be used with a row count-based over window to achieve over window with both time
    range and row count-based range.
    """

    def __init__(self, time_interval: timedelta, agg_func: Callable[[pd.Series], Any]):
        """
        Instantiate a TimeWindowedAggFunction.

        :param time_interval: Only rows with row time in the range of [current_row_time
                              - time_interval, current_row_time] is included in the
                              aggregation function.
        :param agg_func: The name of an aggregation function such as MAX, AVG.
        """
        self.time_interval = time_interval
        self.agg_op = agg_func

    def get_value(self, accumulator: ACC) -> T:
        return accumulator[0]

    def create_accumulator(self) -> ACC:
        return []

    def accumulate(self, accumulator: ACC, *args: pd.Series) -> None:
        df = pd.DataFrame({"val": args[0], "time": args[1]})
        latest_time = df["time"].max()
        df = df[df.time >= latest_time - self.time_interval]
        accumulator.append(self.agg_op(df["val"]))

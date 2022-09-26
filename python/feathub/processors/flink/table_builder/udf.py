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
import glob
import json
import os
from datetime import timedelta
from typing import Dict, Optional, List, Callable, Any

import pandas as pd
from pyflink.table import ScalarFunction, AggregateFunction, StreamTableEnvironment
from pyflink.table.udf import ACC, T

from feathub.common.exceptions import FeathubException
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)


def get_feathub_udf_jar_path() -> str:
    """
    Return the path to the Feathub java udf jar.
    """
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "feathub-udf-*.jar"))
    if len(jars) < 1:
        raise FeathubException(f"Can not find the Feathub udf jar at {lib_dir}.")
    return jars[0]


class JsonStringToMap(ScalarFunction):
    """A scalar function that map a json string to map."""

    def eval(self, s: str) -> Dict:
        return json.loads(s)


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


def register_feathub_java_udf(
    t_env: StreamTableEnvironment, agg_descriptors: List[AggregationFieldDescriptor]
) -> None:
    """
    Register the Feathub java udf to the StreamTableEnvironment.
    """
    agg_funcs = set(descriptor.agg_func for descriptor in agg_descriptors)
    for agg_func in agg_funcs:
        if agg_func == AggFunc.VALUE_COUNTS:
            add_jar_to_t_env(t_env, get_feathub_udf_jar_path())
            t_env.create_java_temporary_function(
                "value_counts", "com.alibaba.feathub.udf.ValueCounts"
            )


def unregister_feathub_java_udf(
    t_env: StreamTableEnvironment, agg_descriptors: List[AggregationFieldDescriptor]
) -> None:
    """
    Unregister the Feathub java udf to the StreamTableEnvironment.
    """
    agg_funcs = set(descriptor.agg_func for descriptor in agg_descriptors)
    for agg_func in agg_funcs:
        if agg_func == AggFunc.VALUE_COUNTS:
            t_env.drop_temporary_function("value_counts")

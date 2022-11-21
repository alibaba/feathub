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
import os
from typing import Optional, TYPE_CHECKING, Dict

from pyflink.table import StreamTableEnvironment

from feathub.common.exceptions import FeathubException
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env

if TYPE_CHECKING:
    from feathub.processors.flink.table_builder.over_window_utils import (
        OverWindowDescriptor,
    )


class JavaUDFDescriptor:
    """
    Descriptor of Feathub Java UDF.
    """

    def __init__(self, udf_name: str, java_class_name: str) -> None:
        self.udf_name = udf_name
        self.java_class_name = java_class_name


def get_feathub_udf_jar_path() -> str:
    """
    Return the path to the Feathub java udf jar.
    """
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "flink-udf-*.jar"))
    if len(jars) < 1:
        raise FeathubException(f"Can not find the Feathub udf jar at {lib_dir}.")
    return jars[0]


def _is_row_and_time_based_over_window(
    over_window_descriptor: Optional["OverWindowDescriptor"],
) -> bool:
    return (
        over_window_descriptor is not None
        and over_window_descriptor.limit is not None
        and over_window_descriptor.window_size is not None
    )


AGG_JAVA_UDF: Dict[AggFunc, JavaUDFDescriptor] = {
    AggFunc.VALUE_COUNTS: JavaUDFDescriptor(
        "VALUE_COUNTS", "com.alibaba.feathub.flink.udf.ValueCountsAggFunc"
    )
}

ROW_AND_TIME_BASED_OVER_WINDOW_JAVA_UDF: Dict[AggFunc, JavaUDFDescriptor] = {
    AggFunc.AVG: JavaUDFDescriptor(
        "TIME_WINDOWED_AVG", "com.alibaba.feathub.flink.udf.TimeWindowedAvgAggFunc"
    ),
    AggFunc.SUM: JavaUDFDescriptor(
        "TIME_WINDOWED_SUM", "com.alibaba.feathub.flink.udf.TimeWindowedSumAggFunc"
    ),
    AggFunc.MIN: JavaUDFDescriptor(
        "TIME_WINDOWED_MIN", "com.alibaba.feathub.flink.udf.TimeWindowedMinAggFunc"
    ),
    AggFunc.MAX: JavaUDFDescriptor(
        "TIME_WINDOWED_MAX", "com.alibaba.feathub.flink.udf.TimeWindowedMaxAggFunc"
    ),
    AggFunc.FIRST_VALUE: JavaUDFDescriptor(
        "TIME_WINDOWED_FIRST_VALUE",
        "com.alibaba.feathub.flink.udf.TimeWindowedFirstValueAggFunc",
    ),
    AggFunc.LAST_VALUE: JavaUDFDescriptor(
        "TIME_WINDOWED_LAST_VALUE",
        "com.alibaba.feathub.flink.udf.TimeWindowedLastValueAggFunc",
    ),
    AggFunc.ROW_NUMBER: JavaUDFDescriptor(
        "TIME_WINDOWED_ROW_NUMBER",
        "com.alibaba.feathub.flink.udf.TimeWindowedRowNumberAggFunc",
    ),
    AggFunc.VALUE_COUNTS: JavaUDFDescriptor(
        "TIME_WINDOWED_VALUE_COUNTS",
        "com.alibaba.feathub.flink.udf.TimeWindowedValueCountsAggFunc",
    ),
}

SCALAR_JAVA_UDF: Dict[str, JavaUDFDescriptor] = {
    "UNIX_TIMESTAMP_MILLIS": JavaUDFDescriptor(
        "UNIX_TIMESTAMP_MILLIS", "com.alibaba.feathub.flink.udf.UnixTimestampMillis"
    ),
}


def register_all_feathub_udf(t_env: StreamTableEnvironment) -> None:
    add_jar_to_t_env(t_env, get_feathub_udf_jar_path())
    for udf_descriptor in {
        *AGG_JAVA_UDF.values(),
        *ROW_AND_TIME_BASED_OVER_WINDOW_JAVA_UDF.values(),
        *SCALAR_JAVA_UDF.values(),
    }:
        t_env.create_java_temporary_function(
            udf_descriptor.udf_name, udf_descriptor.java_class_name
        )

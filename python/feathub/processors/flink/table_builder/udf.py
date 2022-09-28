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
from typing import Optional, List, TYPE_CHECKING, Dict

from pyflink.table import StreamTableEnvironment

from feathub.common.exceptions import FeathubException
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)

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
    jars = glob.glob(os.path.join(lib_dir, "feathub-udf-*.jar"))
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


JAVA_UDF: Dict[AggFunc, JavaUDFDescriptor] = {
    AggFunc.VALUE_COUNTS: JavaUDFDescriptor(
        "value_counts", "com.alibaba.feathub.udf.ValueCountsAggFunc"
    )
}

ROW_AND_TIME_BASED_OVER_WINDOW_JAVA_UDF: Dict[AggFunc, JavaUDFDescriptor] = {
    AggFunc.AVG: JavaUDFDescriptor(
        "time_windowed_avg", "com.alibaba.feathub.udf.TimeWindowedAvgAggFunc"
    ),
    AggFunc.SUM: JavaUDFDescriptor(
        "time_windowed_sum", "com.alibaba.feathub.udf.TimeWindowedSumAggFunc"
    ),
    AggFunc.MIN: JavaUDFDescriptor(
        "time_windowed_min", "com.alibaba.feathub.udf.TimeWindowedMinAggFunc"
    ),
    AggFunc.MAX: JavaUDFDescriptor(
        "time_windowed_max", "com.alibaba.feathub.udf.TimeWindowedMaxAggFunc"
    ),
    AggFunc.FIRST_VALUE: JavaUDFDescriptor(
        "time_windowed_first_value",
        "com.alibaba.feathub.udf.TimeWindowedFirstValueAggFunc",
    ),
    AggFunc.LAST_VALUE: JavaUDFDescriptor(
        "time_windowed_last_value",
        "com.alibaba.feathub.udf.TimeWindowedLastValueAggFunc",
    ),
    AggFunc.ROW_NUMBER: JavaUDFDescriptor(
        "time_windowed_row_number",
        "com.alibaba.feathub.udf.TimeWindowedRowNumberAggFunc",
    ),
    AggFunc.VALUE_COUNTS: JavaUDFDescriptor(
        "time_windowed_value_counts",
        "com.alibaba.feathub.udf.TimeWindowedValueCountsAggFunc",
    ),
}


def register_feathub_java_udf(
    t_env: StreamTableEnvironment,
    agg_descriptors: List[AggregationFieldDescriptor],
    over_window_descriptor: Optional["OverWindowDescriptor"] = None,
) -> None:
    """
    Register the Feathub java udf to the StreamTableEnvironment.
    """
    agg_funcs = set(descriptor.agg_func for descriptor in agg_descriptors)
    for agg_func in agg_funcs:

        if _is_row_and_time_based_over_window(over_window_descriptor):
            udf_descriptor = ROW_AND_TIME_BASED_OVER_WINDOW_JAVA_UDF.get(agg_func, None)
        else:
            udf_descriptor = JAVA_UDF.get(agg_func, None)

        if udf_descriptor is not None:
            add_jar_to_t_env(t_env, get_feathub_udf_jar_path())
            t_env.create_java_temporary_function(
                udf_descriptor.udf_name, udf_descriptor.java_class_name
            )


def unregister_feathub_java_udf(
    t_env: StreamTableEnvironment,
    agg_descriptors: List[AggregationFieldDescriptor],
    over_window_descriptor: Optional["OverWindowDescriptor"] = None,
) -> None:
    """
    Unregister the Feathub java udf to the StreamTableEnvironment.
    """
    agg_funcs = set(descriptor.agg_func for descriptor in agg_descriptors)
    for agg_func in agg_funcs:
        if _is_row_and_time_based_over_window(over_window_descriptor):
            udf_descriptor = ROW_AND_TIME_BASED_OVER_WINDOW_JAVA_UDF.get(agg_func, None)
        else:
            udf_descriptor = JAVA_UDF.get(agg_func, None)

        if udf_descriptor is not None:
            t_env.drop_temporary_function(udf_descriptor.udf_name)

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
from feathub.common.utils import to_java_date_format
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from pyspark.sql import DataFrame as NativeSparkDataFrame, functions

from feathub.common.exceptions import FeathubException


def append_unix_time_attribute_column(
    df: NativeSparkDataFrame,
    timestamp_field: str,
    timestamp_format: str,
) -> NativeSparkDataFrame:
    if EVENT_TIME_ATTRIBUTE_NAME in df.columns:
        raise RuntimeError(
            f"The DataFrame already has column with name "
            f"{EVENT_TIME_ATTRIBUTE_NAME}."
        )
    if timestamp_field is None or timestamp_format is None:
        raise FeathubException(
            "Timestamp filed and format are necessary to "
            "append time attribute column for SparkProcessor."
        )

    if timestamp_format == "epoch":
        return df.withColumn(
            EVENT_TIME_ATTRIBUTE_NAME,
            functions.col(timestamp_field) * functions.lit(1000),
        )
    elif timestamp_format == "epoch_millis":
        return df.withColumn(EVENT_TIME_ATTRIBUTE_NAME, functions.col(timestamp_field))
    else:
        java_datetime_format = to_java_date_format(timestamp_format)
        return df.withColumn(
            EVENT_TIME_ATTRIBUTE_NAME,
            functions.expr(
                f"unix_millis(to_timestamp("
                f"`{timestamp_field}`,'{java_datetime_format}'))"
            ),
        )

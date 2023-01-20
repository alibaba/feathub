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
from typing import Callable, Any, Sequence

import pandas as pd
from pyflink.table import (
    ScalarFunction,
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
)
from pyflink.table.udf import udf

from feathub.common.exceptions import FeathubException
from feathub.common.types import DType
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.flink_types_utils import to_flink_type


def evaluate_python_udf_transform(
    flink_table: NativeFlinkTable,
    transform: PythonUdfTransform,
    result_field_name: str,
    result_type: DType,
) -> NativeFlinkTable:
    """
    Evaluate the Python udf transforms on the given flink table and return the
    result table.

    :param flink_table: The input Flink table.
    :param transform: The Python udf transform.
    :param result_field_name: The name of the result field.
    :param result_type: The data type of the result field.
    """
    field_names = [
        field_name
        for field_name in flink_table.get_schema().get_field_names()
        if field_name != EVENT_TIME_ATTRIBUTE_NAME
    ]
    python_udf = udf(
        _PythonUdfWrapper(field_names, transform.udf),
        result_type=to_flink_type(result_type),
    )
    input_cols = [native_flink_expr.col(field) for field in field_names]
    return flink_table.add_or_replace_columns(
        native_flink_expr.call(python_udf, *input_cols).alias(result_field_name)
    )


class _PythonUdfWrapper(ScalarFunction):
    """
    The wrapper that implement the Flink ScalarFunction.

    The wrapper packages the inputs to a row as Pandas Series and passes to the udf
    of the PythonUdfTransform.
    """

    def __init__(
        self, field_names: Sequence[str], callable_: Callable[[pd.Series], Any]
    ):
        self.field_names = field_names
        self.callable = callable_

    def eval(self, *args: Any) -> Any:
        if len(args) != len(self.field_names):
            raise FeathubException(
                "Number of arguments are not the same as the fields in the input table."
            )

        data = {field_name: value for field_name, value in zip(self.field_names, args)}
        df = pd.Series(data)
        return self.callable(df)

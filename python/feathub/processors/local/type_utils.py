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
from typing import Dict, Type

import numpy as np
import pandas as pd


def cast_series_dtype(series: pd.Series, target_dtype: Type) -> pd.Series:
    """
    Cast the given Pandas Series to the given dtype.
    """
    if series.dtype == target_dtype:
        return series

    if target_dtype == str:
        # Handle str type specially to avoid casting None value to 'None' str
        return series.apply(lambda x: None if x is None else str(x))

    # If we convert the column with NaN value back to integer type e.g., np.int16, we
    # will hit an exception. And we want to keep the type as is.
    if series.hasnans and issubclass(target_dtype, np.integer):
        return series

    return series.astype(target_dtype)


def cast_dataframe_dtype(
    dataframe: pd.DataFrame, target_dtypes: Dict[str, Type]
) -> pd.DataFrame:
    """
    Cast the given Pandas DataFrame to the given dtypes. The dtypes is a mapping from
    the column names to the target data type.
    """
    for field_name, target_dtype in target_dtypes.items():
        if field_name not in dataframe:
            continue

        dataframe[field_name] = cast_series_dtype(dataframe[field_name], target_dtype)

    return dataframe

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
from typing import Sequence, Any, Callable, Dict

import numpy as np
import pandas as pd

from feathub.feature_views.transforms.agg_func import AggFunc


def _value_counts(inputs: Sequence[Any]) -> Any:
    if len(inputs) <= 0:
        return None
    return dict(pd.Series(inputs).value_counts())


def _collect_list(inputs: Sequence[Any]) -> Any:
    if len(inputs) <= 0:
        return None
    return list(inputs)


AGG_FUNCTIONS: Dict[AggFunc, Callable[[Sequence[Any]], Any]] = {
    AggFunc.AVG: np.mean,
    AggFunc.SUM: np.sum,
    AggFunc.MAX: lambda l: None if len(l) <= 0 else np.max(l),
    AggFunc.MIN: lambda l: None if len(l) <= 0 else np.min(l),
    AggFunc.FIRST_VALUE: lambda l: None if len(l) <= 0 else l[0],
    AggFunc.LAST_VALUE: lambda l: None if len(l) <= 0 else l[-1],
    AggFunc.ROW_NUMBER: lambda l: len(l),
    AggFunc.COUNT: lambda l: len(l),
    AggFunc.VALUE_COUNTS: _value_counts,
    AggFunc.COLLECT_LIST: _collect_list,
}

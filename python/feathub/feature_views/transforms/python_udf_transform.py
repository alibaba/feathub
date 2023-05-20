# Copyright 2022 The FeatHub Authors
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
import base64
from typing import Callable, Any, Dict

import cloudpickle
import pandas as pd

from feathub.common.utils import append_metadata_to_json
from feathub.feature_views.transforms.transformation import Transformation


class PythonUdfTransform(Transformation):
    """
    Derives feature values by applying a Python UDF on one row of the parent table at a
    time.
    """

    # TODO: Validate the type of default value with feature type.
    def __init__(
        self,
        udf: Callable[[pd.Series], Any],
        fail_on_exception: bool = True,
        value_on_exception: Any = None,
    ) -> None:
        """
        :param udf: The udf that will be invoked for each row. The input
                          of the udf is a Pandas Series object that represent the
                          row.
        :param fail_on_exception: Whether to fail the job in case of an exception is
                                  raised by the udf. If this is false, the
                                  `value_on_exception` is used in case of exception.
        :param value_on_exception: If `fail_on_exception` is set to false, this is the
                              default value of the result when an exception is raised.
        """
        super().__init__()
        self.original_udf = udf
        self.fail_on_exception = fail_on_exception
        self.value_on_exception = value_on_exception

    @property
    def udf(self) -> Callable[[pd.Series], Any]:
        if self.fail_on_exception:
            return self.original_udf

        return self._wrap_udf_with_value_on_exception(
            self.original_udf, self.value_on_exception
        )

    @staticmethod
    def _wrap_udf_with_value_on_exception(
        udf: Callable[[pd.Series], Any], default_value: Any
    ) -> Callable[[pd.Series], Any]:
        def wrapper(s: pd.Series) -> Any:
            try:
                return udf(s)
            except Exception:
                return default_value

        return wrapper

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "udf": base64.encodebytes(cloudpickle.dumps(self.original_udf)).decode(),
            "fail_on_exception": self.fail_on_exception,
            "value_on_exception": self.value_on_exception,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PythonUdfTransform":
        return PythonUdfTransform(
            udf=cloudpickle.loads(base64.decodebytes(json_dict["udf"].encode())),
            fail_on_exception=json_dict["fail_on_exception"],
            value_on_exception=json_dict["value_on_exception"],
        )

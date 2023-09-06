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
import glob
import os
from typing import Dict

from pyflink.table import StreamTableEnvironment

from feathub.common.exceptions import FeathubException
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env


class JavaUDFDescriptor:
    """
    Descriptor of FeatHub Java UDF.
    """

    def __init__(self, udf_name: str, java_class_name: str) -> None:
        self.udf_name = udf_name
        self.java_class_name = java_class_name


def get_feathub_udf_jar_path() -> str:
    """
    Return the path to the FeatHub java udf jar.
    """
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "flink-udf-*.jar"))
    if len(jars) < 1:
        raise FeathubException(f"Can not find the FeatHub udf jar at {lib_dir}.")
    return jars[0]


SCALAR_JAVA_UDF: Dict[str, JavaUDFDescriptor] = {
    # TODO: Introduce a UDF for UNIX_TIMESTAMP that throw exception on illegal data.
    "UNIX_TIMESTAMP_MILLIS": JavaUDFDescriptor(
        "UNIX_TIMESTAMP_MILLIS", "com.alibaba.feathub.flink.udf.UnixTimestampMillis"
    ),
}


def register_all_feathub_udf(t_env: StreamTableEnvironment) -> None:
    add_jar_to_t_env(t_env, get_feathub_udf_jar_path())
    for udf_descriptor in {
        *SCALAR_JAVA_UDF.values(),
    }:
        t_env.create_java_temporary_function(
            udf_descriptor.udf_name, udf_descriptor.java_class_name
        )

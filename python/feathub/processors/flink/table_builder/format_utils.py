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
from typing import Dict, Any

from pyflink.table import StreamTableEnvironment

from feathub.common.config import BaseConfig
from feathub.common.exceptions import FeathubException
from feathub.feature_tables.format_config import (
    ProtobufConfig,
    PROTOBUF_JAR_PATH_CONFIG,
    PROTOBUF_CLASS_NAME_CONFIG,
    IGNORE_PARSE_ERRORS_CONFIG,
    JsonConfig,
    CsvConfig,
    DataFormat,
)
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env


def load_format(
    t_env: StreamTableEnvironment,
    data_format: str,
    data_format_props: Dict[str, Any],
) -> None:
    if data_format == DataFormat.JSON or data_format == DataFormat.CSV:
        return
    elif data_format == DataFormat.PROTOBUF:
        config = ProtobufConfig(data_format_props)
        protobuf_jar_path = config.get(PROTOBUF_JAR_PATH_CONFIG)
        _load_protobuf_format_jar(t_env, protobuf_jar_path)
        return
    elif data_format == DataFormat.PARQUET:
        _load_parquet_format_jar(t_env)
        return

    raise FeathubException(f"Unsupported format {data_format}.")


def get_flink_format_config(
    data_format: str, data_format_props: Dict[str, Any]
) -> Dict[str, str]:
    if data_format == DataFormat.JSON:
        config: BaseConfig = JsonConfig(data_format_props)
        return {"json.ignore-parse-errors": str(config.get(IGNORE_PARSE_ERRORS_CONFIG))}
    elif data_format == DataFormat.CSV:
        config = CsvConfig(data_format_props)
        return {"csv.ignore-parse-errors": str(config.get(IGNORE_PARSE_ERRORS_CONFIG))}
    elif data_format == DataFormat.PROTOBUF:
        config = ProtobufConfig(data_format_props)
        return {
            "protobuf.message-class-name": config.get(PROTOBUF_CLASS_NAME_CONFIG),
            "protobuf.ignore-parse-errors": str(config.get(IGNORE_PARSE_ERRORS_CONFIG)),
        }
    elif data_format == DataFormat.PARQUET:
        return {}

    raise FeathubException(f"Unsupported format {data_format}.")


def _load_protobuf_format_jar(
    t_env: StreamTableEnvironment, protobuf_jar_path: str
) -> None:
    avro_jar_path = _get_jar_path("flink-sql-protobuf-*.jar")
    add_jar_to_t_env(t_env, avro_jar_path, protobuf_jar_path)


def _load_parquet_format_jar(t_env: StreamTableEnvironment) -> None:
    add_jar_to_t_env(t_env, _get_jar_path("flink-sql-parquet-*.jar"))


def _get_jar_path(jar_files_pattern: str) -> str:
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, jar_files_pattern))
    if len(jars) < 1:
        raise FeathubException(
            f"Can not find the jar at {lib_dir} with pattern {jar_files_pattern}."
        )
    return jars[0]

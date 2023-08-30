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
from typing import Dict, Any

from feathub.common.config import BaseConfig, ConfigDef
from feathub.common.validators import not_none


class DataFormat:
    CSV = "csv"
    JSON = "json"
    PROTOBUF = "protobuf"
    PARQUET = "parquet"


IGNORE_PARSE_ERRORS_CONFIG = "ignore_parse_error"
IGNORE_PARSE_ERRORS_DOC = (
    "Skip fields and rows with parse errors instead of failing. Fields are set to "
    "null in case of errors."
)

ignore_parse_error_config_def = ConfigDef(
    name=IGNORE_PARSE_ERRORS_CONFIG,
    value_type=bool,
    description=IGNORE_PARSE_ERRORS_DOC,
    default_value=True,
)


class CsvConfig(BaseConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(
            [
                # TODO: Support more csv config
                ignore_parse_error_config_def,
            ]
        )


class JsonConfig(BaseConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(
            [
                # TODO: Support more Json config
                ignore_parse_error_config_def,
            ]
        )


PROTOBUF_JAR_PATH_CONFIG = "protobuf.jar_path"
PROTOBUF_JAR_PATH_DOC = (
    "The path to the jar that contains the Protobuf generated classes."
)

PROTOBUF_CLASS_NAME_CONFIG = "protobuf.class_name"
PROTOBUF_CLASS_NAME_DOC = (
    "The full name of a Protobuf generated class. The name must match the message name "
    "in the proto definition file. $ is supported for inner class names, like "
    "'com.example.OuterClass$MessageClass'"
)


class ProtobufConfig(BaseConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(
            [
                ConfigDef(
                    name=PROTOBUF_JAR_PATH_CONFIG,
                    value_type=str,
                    description=PROTOBUF_JAR_PATH_DOC,
                    validator=not_none(),
                ),
                ConfigDef(
                    name=PROTOBUF_CLASS_NAME_CONFIG,
                    value_type=str,
                    description=PROTOBUF_CLASS_NAME_DOC,
                    validator=not_none(),
                ),
                ignore_parse_error_config_def,
            ]
        )

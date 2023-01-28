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
from typing import TypeVar, Generic, Optional, Dict, Type, Any, List

import pandas as pd
import tzlocal

from feathub.common.exceptions import FeathubConfigurationException
from feathub.common.validators import Validator


def flatten_dict(dict_to_flatten: Dict) -> Dict:
    """
    Flatten the given dictionary.

    E.g

    {
        "processor": {
            "type": "flink",
            "flink": {
                "rest.address": "localhost",
            },
        },
        "online_store": {
            "types": ["memory"],
            "memory": {},
        },
    }

    becomes

    {
        "processor.type": "flink",
        "processor.flink.rest.address": "localhost",
        "online_store.types": ["memory"],
    }
    """
    json_normalized = pd.json_normalize(dict_to_flatten).to_dict(orient="records")
    if len(json_normalized) >= 1:
        return json_normalized[0]
    else:
        return {}


T = TypeVar("T")


class ConfigDef(Generic[T]):
    """
    ConfigDef describes a configuration parameter.

    It encapsulates the configuration name, description of the configuration, data type
    of the value, an optional default value, and an optional validator for the
    configuration parameter.
    """

    def __init__(
        self,
        name: str,
        value_type: Type[T],
        description: str,
        default_value: Optional[T] = None,
        validator: Optional[Validator[T]] = None,
    ):
        """
        :param name: The name of the configuration.
        :param description: The description of the configuration.
        :param value_type: The python type of the value of the configuration.
        :param default_value: The default value of the configuration.
        :param validator: Optional. If it is not None, it is the validator that use to
                          validate the value of the config.
        """
        self.name = name
        self.default_value = default_value
        self.description = description
        self.value_type = value_type
        self.validator = validator


COMMON_PREFIX = "common."

TIMEZONE_CONFIG = COMMON_PREFIX + "timeZone"
TIMEZONE_DOC = (
    "The Region ID that represents a timezone to be used during "
    "timestamp-related data type conversion operations. Region IDs "
    "must have the form 'area/city', such as 'America/Los_Angeles'."
)

# TODO: Add document describing the configuration options available
#  in Feathub.
common_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=TIMEZONE_CONFIG,
        value_type=str,
        description=TIMEZONE_DOC,
        default_value=str(tzlocal.get_localzone()),
    )
]


class BaseConfig:
    """
    A convenient base class for configurations to extend.

    This class holds both the original configuration that was provided and the parsed.
    """

    def __init__(self, original_props: Dict[str, Any]) -> None:
        """
        :param original_props: The original properties.
        """
        self.original_props = original_props
        self.config_values: Dict[str, Any] = {}
        self.update_config_values(common_config_defs)

    def get(self, key: str) -> Any:
        """
        Get the value of the config with the given key.

        :param key: Key of the config
        :return: The value of the config.
        """
        if key not in self.config_values:
            raise FeathubConfigurationException(f"Unknown configuration {key}.")
        return self.config_values[key]

    def original_props_with_prefix(self, prefix: str, strip: bool) -> Dict[str, Any]:
        """
        Gets all original properties with the given prefix.

        :param prefix: The prefix to use as a filter.
        :param strip: Whether to strip the prefix before adding to the output.
        :return: A Dict containing the settings with the prefix
        """
        res = {}
        prefix_len = len(prefix)
        for k, v in self.original_props.items():
            if not k.startswith(prefix):
                continue
            if strip:
                res[k[prefix_len:]] = v
            else:
                res[k] = v

        return res

    def __repr__(self) -> str:
        return self.config_values.__repr__()

    def __str__(self) -> str:
        return self.config_values.__str__()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and self.config_values.__eq__(
            other.config_values
        )

    def update_config_values(self, config_defs: List[ConfigDef]) -> None:
        for config_def in config_defs:
            if config_def.name in self.config_values:
                raise FeathubConfigurationException(
                    f"Duplicate configuration of {config_def.name} is detected."
                )
            value = self.original_props.get(config_def.name, config_def.default_value)
            if value is not None and not isinstance(value, config_def.value_type):
                raise FeathubConfigurationException(
                    f"Configuration type error: {config_def.name} expects type "
                    f"{config_def.value_type} but got {type(value)}."
                )
            if config_def.validator is not None:
                config_def.validator.ensure_valid(config_def.name, value)
            self.config_values[config_def.name] = value

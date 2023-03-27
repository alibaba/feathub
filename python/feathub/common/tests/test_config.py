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
import unittest
from typing import List, Dict, Any

from feathub.common.config import (
    ConfigDef,
    BaseConfig,
    flatten_dict,
)
from feathub.common.exceptions import FeathubConfigurationException
from feathub.common.validators import in_list, is_subset


class MockConfig(BaseConfig):
    def __init__(self, config_defs: List[ConfigDef], props: Dict[str, Any]):
        super().__init__(props)
        self.update_config_values(config_defs)


class ConfigTest(unittest.TestCase):
    def test_flatten_dict(self):
        config = {
            "processor": {
                "type": "flink",
                "flink": {
                    "rest.address": "localhost",
                    "rest.port": "8081",
                    "flink.default.parallelism": "2",
                },
            },
            "online_store": {
                "types": ["memory"],
                "memory": {},
            },
            "registry": {
                "type": "local",
                "local": {
                    "namespace": "default",
                },
            },
            "feature_service": {
                "type": "local",
                "local": {},
            },
        }
        result_dict = flatten_dict(config)

        expected_result = {
            "processor.type": "flink",
            "processor.flink.rest.address": "localhost",
            "processor.flink.rest.port": "8081",
            "processor.flink.flink.default.parallelism": "2",
            "online_store.types": ["memory"],
            "registry.type": "local",
            "registry.local.namespace": "default",
            "feature_service.type": "local",
        }
        self.assertEqual(expected_result, result_dict)

        self.assertEqual({}, flatten_dict({}))

    def test_configuration_get(self):
        config_options = [
            ConfigDef(
                name="a.b",
                value_type=int,
                description="a_b",
                default_value=1,
                validator=None,
            )
        ]
        config = MockConfig(config_options, {"a.b": 2})
        self.assertEqual(2, config.get("a.b"))

        config = MockConfig(config_options, {})
        self.assertEqual(1, config.get("a.b"))

        with self.assertRaises(FeathubConfigurationException) as cm:
            config.get("a.c")

        self.assertIn("Unknown configuration a.c", cm.exception.args[0])

    def test_config_type_error(self):
        config_options = [
            ConfigDef(name="a.b", value_type=int, description="a_b", validator=None)
        ]

        with self.assertRaises(FeathubConfigurationException) as cm:
            MockConfig(config_options, {"a.b": "string"})

        self.assertIn("Configuration type error:", cm.exception.args[0])

    def test_config_with_validator(self):
        config_options: List[ConfigDef] = [
            ConfigDef(
                name="a.b",
                value_type=str,
                description="a_b",
                default_value="a",
                validator=in_list("a", "b"),
            ),
            ConfigDef(
                name="a.c",
                value_type=list,
                description="a_c",
                default_value=["a", "b"],
                validator=is_subset("a", "b"),
            ),
        ]

        config = MockConfig(config_options, {"a.b": "b"})
        self.assertEqual("b", config.get("a.b"))
        self.assertEqual(["a", "b"], config.get("a.c"))

        with self.assertRaises(FeathubConfigurationException) as cm:
            MockConfig(config_options, {"a.b": "c"})
        self.assertIn("Invalid value c of a.b", cm.exception.args[0])

        with self.assertRaises(FeathubConfigurationException) as cm:
            MockConfig(config_options, {"a.c": ["a", "b", "c"]})
        self.assertIn("Invalid value c of a.c", cm.exception.args[0])

    def test_originals_with_prefix(self):
        config = MockConfig([], {"a.b": "b", "a.b.a": "a", "a.b.b": "b", "a.c.c": "c"})
        self.assertEqual(
            {"a": "a", "b": "b"}, config.original_props_with_prefix("a.b.", True)
        )

        self.assertEqual(
            {"a.b.a": "a", "a.b.b": "b"},
            config.original_props_with_prefix("a.b.", False),
        )

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
from typing import Dict, Optional

from feathub.feature_tables.sinks.sink import Sink


class KafkaSink(Sink):
    def __init__(
        self,
        bootstrap_server: str,
        topic: str,
        key_format: Optional[str],
        value_format: str,
        producer_properties: Optional[Dict[str, str]] = None,
    ):
        """
        :param bootstrap_server: Comma separated list of Kafka brokers.
        :param topic: The topic name to write the data.
        :param key_format: Optional. If it is not None, it is the format used to
                           serialize the keys of the upstream table and set to the key
                           part of the output Kafka message, e.g. csv, json, raw, etc.
                           If it is None, the keys of the upstream table are set in the
                           value part of the output Kafka record.
        :param value_format: The format used to serialize the upstream table to the
                             value part of the output Kafka messages, e.g. csv, json,
                             etc.
        :param producer_properties: Optional. If it is not None, it contains the extra
                                    kafka producer properties.
        """
        super().__init__(
            name="",
            system_name="kafka",
            properties={"bootstrap_server": bootstrap_server, "topic": topic},
            data_format=value_format,
        )
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.key_format = key_format
        self.value_format = value_format
        self.producer_properties = (
            {} if producer_properties is None else producer_properties
        )

    def to_json(self) -> Dict:
        return {
            "type": "KafkaSink",
            "bootstrap_server": self.bootstrap_server,
            "topic": self.topic,
            "key_format": self.key_format,
            "value_format": self.value_format,
            "producer_properties": self.producer_properties,
        }

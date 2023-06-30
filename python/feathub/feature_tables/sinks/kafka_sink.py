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
from typing import Dict, Optional, Any

from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.sinks.sink import Sink


class KafkaSink(Sink):
    def __init__(
        self,
        bootstrap_server: str,
        topic: str,
        key_format: Optional[str],
        value_format: str,
        producer_props: Optional[Dict[str, str]] = None,
        key_data_format_props: Optional[Dict[str, Any]] = None,
        value_data_format_props: Optional[Dict[str, Any]] = None,
        keep_timestamp_field: bool = True,
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
        :param producer_props: Optional. If it is not None, it contains the extra
                                    kafka producer properties.
        :param key_data_format_props: Optional. The properties of the format for
                                           Kafka message key.
        :param value_data_format_props: Optional. The properties of the format for
                                             Kafka message value.
        :param keep_timestamp_field: True if the timestamp field of the feature table
                                     should be persisted to the external system through
                                     the sink.
        """
        super().__init__(
            name="",
            system_name="kafka",
            table_uri={"bootstrap_server": bootstrap_server, "topic": topic},
            keep_timestamp_field=keep_timestamp_field,
        )
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.key_format = key_format
        self.key_format_props = (
            {} if key_data_format_props is None else key_data_format_props
        )
        self.value_format = value_format
        self.value_format_props = (
            {} if value_data_format_props is None else value_data_format_props
        )
        self.producer_props = {} if producer_props is None else producer_props

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "bootstrap_server": self.bootstrap_server,
            "topic": self.topic,
            "key_format": self.key_format,
            "value_format": self.value_format,
            "producer_props": self.producer_props,
            "key_data_format_props": self.key_format_props,
            "value_data_format_props": self.value_format_props,
            "keep_timestamp_field": self.keep_timestamp_field,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "KafkaSink":
        return KafkaSink(
            bootstrap_server=json_dict["bootstrap_server"],
            topic=json_dict["topic"],
            key_format=json_dict["key_format"],
            value_format=json_dict["value_format"],
            producer_props=json_dict["producer_props"],
            key_data_format_props=json_dict["key_data_format_props"],
            value_data_format_props=json_dict["value_data_format_props"],
            keep_timestamp_field=json_dict["keep_timestamp_field"],
        )

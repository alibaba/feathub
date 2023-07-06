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
from copy import deepcopy
from datetime import timedelta, datetime
from typing import Dict, Optional, List, Any

from feathub.common.exceptions import FeathubException
from feathub.common.utils import from_json, append_metadata_to_json
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class KafkaSource(FeatureTable):
    """A source that reads data from kafka."""

    def __init__(
        self,
        name: str,
        bootstrap_server: str,
        topic: str,
        key_format: Optional[str],
        value_format: str,
        schema: Schema,
        consumer_group: str,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        consumer_props: Optional[Dict[str, str]] = None,
        max_out_of_orderness: timedelta = timedelta(0),
        startup_mode: str = "group-offsets",
        startup_datetime: Optional[datetime] = None,
        partition_discovery_interval: timedelta = timedelta(minutes=5),
        is_bounded: bool = False,
        key_data_format_props: Optional[Dict[str, Any]] = None,
        value_data_format_props: Optional[Dict[str, Any]] = None,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param bootstrap_server: Comma separated list of Kafka brokers.
        :param topic: Topic name(s) to read data from. It supports topic list by
                      separating topic by semicolon like 'topic-1;topic-2'.
        :param key_format: Optional. If it is not None, it is the format used to
                           deserialize the key part of Kafka message, e.g. csv, json,
                           raw, etc. If it is None, we assume the Kafka message does
                           not have a key.
        :param value_format: The format used to deserialize the value part of Kafka
                            messages, e.g. csv, json, raw, etc.
        :param schema: The schema of the data.
        :param consumer_group: The id of the consumer group for Kafka consumer.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field. See TableDescriptor
                                 for valid format values. Only effective when the
                                 `timestamp_field` is not None.
        :param consumer_props: Optional. If it is not None, it contains the extra
                                    kafka consumer properties.
        :param max_out_of_orderness: The maximum amount of time a record is allowed to
                                     be late. Default is 0 second, meaning the records
                                     should be ordered by `timestamp_field`.
        :param startup_mode: Startup mode for Kafka consumer, valid values are
                             'earliest-offset', 'latest-offset', 'group-offsets', and
                             'timestamp'. Default to 'group-offsets'.
                             - `group-offsets`: start from committed offsets in ZK /
                                                Kafka brokers of the consumer group.
                             - `earliest-offset`: start from the earliest offset
                                                  possible.
                             - `latest-offset`: start from the latest offset.
                             - `timestamp`: start from user-supplied timestamp for all
                                            partitions.
        :param startup_datetime: Only required and used when startup_mode is set to
                                 'timestamp'. It specifies the start datetime for all
                                 partitions.
        :param partition_discovery_interval: Interval for consumer to periodically
                                             discover dynamically created partitions.
                                             Default to 5 minutes so that it is
                                             consistent with the 'metadata.max.age.ms'
                                             for Kafka Consumer config.
        :param is_bounded: Whether the KafkaSource should be bounded. If the KafkaSource
                           is bounded, it stops at the latest offsets of the partitions
                           when the KafkaSource starts to run.
        :param key_data_format_props: Optional. The properties of the format for
                                           Kafka message key.
        :param value_data_format_props: Optional. The properties of the format for
                                             Kafka message value.
        """
        super().__init__(
            name=name,
            system_name="kafka",
            table_uri={"bootstrap_server": bootstrap_server, "topic": topic},
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            schema=schema,
        )
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.key_format = key_format
        self.key_data_format_props = (
            {} if key_data_format_props is None else key_data_format_props
        )
        self.value_format = value_format
        self.value_data_format_props = (
            {} if value_data_format_props is None else value_data_format_props
        )
        self.consumer_group = consumer_group
        self.consumer_props = {} if consumer_props is None else consumer_props
        self.max_out_of_orderness = max_out_of_orderness
        self.startup_mode = startup_mode
        self.startup_datetime = startup_datetime
        self.partition_discovery_interval = partition_discovery_interval
        self._is_bounded = is_bounded

        if startup_mode == "timestamp" and startup_datetime is None:
            raise FeathubException(
                "startup_datetime is required when startup_mode is timestamp."
            )

    def is_bounded(self) -> bool:
        return self._is_bounded

    def get_bounded_view(self) -> TableDescriptor:
        if self.is_bounded():
            return self
        kafka_source = deepcopy(self)
        kafka_source._is_bounded = True
        return kafka_source

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "bootstrap_server": self.bootstrap_server,
            "topic": self.topic,
            "key_format": self.key_format,
            "value_format": self.value_format,
            "schema": None if self.schema is None else self.schema.to_json(),
            "consumer_group": self.consumer_group,
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "consumer_props": self.consumer_props,
            "max_out_of_orderness_ms": self.max_out_of_orderness
            / timedelta(milliseconds=1),
            "startup_mode": self.startup_mode,
            "startup_datetime_ms": None
            if self.startup_datetime is None
            else int(self.startup_datetime.timestamp() * 1000),
            "partition_discovery_interval_ms": self.partition_discovery_interval
            / timedelta(milliseconds=1),
            "is_bounded": self.is_bounded(),
            "key_data_format_props": self.key_data_format_props,
            "value_data_format_props": self.value_data_format_props,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "KafkaSource":
        return KafkaSource(
            name=json_dict["name"],
            bootstrap_server=json_dict["bootstrap_server"],
            topic=json_dict["topic"],
            key_format=json_dict["key_format"],
            value_format=json_dict["value_format"],
            schema=from_json(json_dict["schema"])
            if json_dict["schema"] is not None
            else None,
            consumer_group=json_dict["consumer_group"],
            keys=json_dict["keys"],
            timestamp_field=json_dict["timestamp_field"],
            timestamp_format=json_dict["timestamp_format"],
            consumer_props=json_dict["consumer_props"],
            max_out_of_orderness=timedelta(
                milliseconds=json_dict["max_out_of_orderness_ms"]
            ),
            startup_mode=json_dict["startup_mode"],
            startup_datetime=datetime.fromtimestamp(
                json_dict["startup_datetime_ms"] / 1000.0
            )
            if json_dict["startup_datetime_ms"] is not None
            else None,
            partition_discovery_interval=timedelta(
                milliseconds=json_dict["partition_discovery_interval_ms"]
            ),
            is_bounded=json_dict["is_bounded"],
            key_data_format_props=json_dict["key_data_format_props"],
            value_data_format_props=json_dict["value_data_format_props"],
        )

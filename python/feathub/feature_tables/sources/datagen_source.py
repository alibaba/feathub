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
from datetime import timedelta
from typing import Dict, Optional, List, Union, Any

from feathub.common.exceptions import FeathubException
from feathub.common.utils import from_json, append_metadata_to_json
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor

DEFAULT_BOUNDED_NUMBER_OF_ROWS = 100


class RandomField:
    """
    The field config of a randomly generated field.
    """

    def __init__(
        self,
        minimum: Optional[Any] = None,
        maximum: Optional[Any] = None,
        max_past: timedelta = timedelta(0),
        length: int = 100,
    ) -> None:
        """
        :param minimum: Optional. If it is not None, it specifies the minimum value of
                        random generated field, work for numeric types. If it is None,
                        it uses the minimum value of the field type.
        :param maximum: Optional. If it is not None, it specifies the maximum value of
                        random generated field, work for numeric types. If it is None,
                        it uses the maximum value of the field type.
        :param max_past: It specifies the maximum past of a timestamp field,
                         only works for Timestamp type.
        :param length: Size or length of field type String or VectorType. Default to
                       100.
        """
        self.minimum = minimum
        self.maximum = maximum
        self.max_past = max_past
        self.length = length

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "minimum": self.minimum,
            "maximum": self.maximum,
            "max_past_ms": self.max_past / timedelta(milliseconds=1),
            "length": self.length,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "RandomField":
        return RandomField(
            minimum=json_dict["minimum"],
            maximum=json_dict["maximum"],
            max_past=timedelta(milliseconds=json_dict["max_past_ms"]),
            length=json_dict["length"],
        )


class SequenceField:
    """
    The field config of a sequentially generated field.
    """

    def __init__(self, start: Any, end: Any) -> None:
        """
        :param start: Start value of sequence generator(inclusive).
        :param end: End value of sequence generator(inclusive).
        """
        self.start = start
        self.end = end

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {"start": self.start, "end": self.end}

    @classmethod
    def from_json(cls, json_dict: Dict) -> "SequenceField":
        return SequenceField(
            start=json_dict["start"],
            end=json_dict["end"],
        )


class DataGenSource(FeatureTable):
    """
    DataGenSource generates table with random data or sequential data.

    This source is bounded if number_of_rows is not None, or any field is a
    SequenceField. If number_of_rows is set and the source also contains
    SequenceFields, the actually generated number of rows would be decided
    by the minimum span among these configurations.

    FeatHub types that can be used in DataGenSource's schema and their
    supported field config:

    - String:       RandomField / SequenceField
    - Bool:         RandomField
    - Int32:        RandomField / SequenceField
    - Int64:        RandomField / SequenceField
    - Float32:      RandomField / SequenceField
    - Float64:      RandomField / SequenceField
    - Timestamp:    RandomField
    - VectorType:   RandomField
    - MapType:      RandomField
    """

    def __init__(
        self,
        name: str,
        schema: Schema,
        rows_per_second: int = 10000,
        number_of_rows: Optional[int] = None,
        field_configs: Optional[Dict[str, Union[RandomField, SequenceField]]] = None,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        max_out_of_orderness: timedelta = timedelta(0),
    ) -> None:
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param schema: The schema of the data.
        :param rows_per_second: Rows per second to control the emit rate when the source
                                is unbounded.
        :param number_of_rows: The number of rows that would be generated.
        :param field_configs: A Map of field to the config of the field. The config
                              can be either RandomField or SequenceField. Every field
                              should be in the schema of the DataGenSource. If a field
                              in the schema doesn't have a config, it is set to
                              RandomField.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field. See TableDescriptor
                                 for valid format values. Only effective when the
                                 `timestamp_field` is not None.
        :param max_out_of_orderness: The maximum amount of time a record is allowed to
                                     be late. Default is 0 second, meaning the records
                                     should be ordered by `timestamp_field`.
        """
        super().__init__(
            name=name,
            system_name="datagen",
            table_uri={},
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            schema=schema,
        )

        self.number_of_rows = number_of_rows
        self.rows_per_second = rows_per_second
        self.field_configs = field_configs if field_configs is not None else {}
        self.max_out_of_orderness = max_out_of_orderness

        for field_name, _ in self.field_configs.items():
            if field_name not in schema.field_names:
                raise FeathubException(f"Field {field_name} is not in the schema.")

        for field_name in schema.field_names:
            if field_name not in self.field_configs:
                self.field_configs[field_name] = RandomField()

        for _, field in self.field_configs.items():
            if isinstance(field, SequenceField):
                field_size = field.end - field.start + 1
                if self.number_of_rows is None:
                    self.number_of_rows = field_size
                else:
                    self.number_of_rows = min(self.number_of_rows, field_size)

    def is_bounded(self) -> bool:
        return self.number_of_rows is not None

    def get_bounded_view(self) -> TableDescriptor:
        if self.is_bounded():
            return self

        data_gen_source = deepcopy(self)
        data_gen_source.number_of_rows = DEFAULT_BOUNDED_NUMBER_OF_ROWS
        return data_gen_source

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "schema": None if self.schema is None else self.schema.to_json(),
            "rows_per_second": self.rows_per_second,
            "number_of_rows": self.number_of_rows,
            "field_configs": {k: v.to_json() for k, v in self.field_configs.items()},
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "max_out_of_orderness_ms": self.max_out_of_orderness
            / timedelta(milliseconds=1),
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "DataGenSource":
        return DataGenSource(
            name=json_dict["name"],
            schema=from_json(json_dict["schema"])
            if json_dict["schema"] is not None
            else None,
            rows_per_second=json_dict["rows_per_second"],
            number_of_rows=json_dict["number_of_rows"],
            field_configs={
                k: from_json(v) for k, v in json_dict["field_configs"].items()
            },
            keys=json_dict["keys"],
            timestamp_field=json_dict["timestamp_field"],
            timestamp_format=json_dict["timestamp_format"],
            max_out_of_orderness=timedelta(
                milliseconds=json_dict["max_out_of_orderness_ms"]
            ),
        )

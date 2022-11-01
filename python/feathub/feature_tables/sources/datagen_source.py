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
from datetime import timedelta
from typing import Dict, Optional, List, Union, Any

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema


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
                         only works for timestamp types.
        :param length: Size or length of field type String or VectorType. Default to
                       100.
        """
        self.minimum = minimum
        self.maximum = maximum
        self.max_past = max_past
        self.length = length

    def to_json(self) -> Dict:
        return {
            "type": "random",
            "minimum": self.minimum,
            "maximum": self.maximum,
            "max_past": self.max_past,
            "length": self.length,
        }


class SequenceField:
    """
    The field config of a sequentially generated field.
    """

    def __init__(self, start: Any, end: Any) -> None:
        """
        :param start: Start value of sequence generator.
        :param end: End value of sequence generator.
        """
        self.start = start
        self.end = end

    def to_json(self) -> Dict:
        return {"type": "sequence", "start": self.start, "end": self.end}


default_field_config = RandomField()


class DataGenConfig:
    """
    DataGenConfig specifies how the data are generated.
    """

    def __init__(
        self,
        rows_per_second: int = 10000,
        number_of_rows: Optional[int] = None,
        field_configs: Optional[Dict[str, Union[RandomField, SequenceField]]] = None,
    ) -> None:
        """
        :param rows_per_second: Rows per second to control the emit rate.
        :param number_of_rows: Optional. If it is None, unlimited number of rows will be
                               generated. If it is not None, it specifies the total
                               number of rows to emit.
        :param field_configs: A Map of field to the config of the field. The config
                              can be either RandomField or SequenceField. Every field
                              should be in the schema of the DataGenSource. If a field
                              in the schema doesn't have a config, it is set to
                              `default_field_config`.
        """
        self.rows_per_second = rows_per_second
        self.number_of_rows = number_of_rows
        self.field_configs = field_configs

    def to_json(self) -> Dict:
        return {
            "rows_per_second": self.rows_per_second,
            "number_of_rows": self.number_of_rows,
            "field_configs": {k: v.to_json() for k, v in self.field_configs.items()},
        }


class DataGenSource(FeatureTable):
    """
    DataGenSource generate table with random data or sequential data.
    """

    def __init__(
        self,
        name: str,
        schema: Schema,
        data_gen_config: DataGenConfig = DataGenConfig(),
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        max_out_of_orderness: timedelta = timedelta(0),
    ) -> None:
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param schema: The schema of the data.
        :param data_gen_config: The DataGenConfig that specify how the data are
                                generated.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field.
        :param max_out_of_orderness: The maximum amount of time a record is allowed to
                                     be late. Default is 0 second, meaning the records
                                     should be ordered by `timestamp_field`.
        """
        super().__init__(
            name,
            "datagen",
            {},
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            schema=schema,
        )

        self.data_gen_config = data_gen_config
        self.max_out_of_orderness = max_out_of_orderness

        # TODO: Add validation of field type and field config.
        for field, _ in data_gen_config.field_configs.items():
            if field not in schema.field_names:
                raise FeathubException(f"Field {field} is not in the schema.")

        for field in schema.field_names:
            if field not in data_gen_config.field_configs:
                data_gen_config.field_configs[field] = default_field_config

    def to_json(self) -> Dict:
        return {
            "type": "DataGenSource",
            "name": self.name,
            "schema": self.schema,
            "data_gen_config": self.data_gen_config.to_json(),
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "max_out_of_orderness_ms": self.max_out_of_orderness
            / timedelta(milliseconds=1),
        }

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

from typing import Optional, List, Dict

from feathub.feature_views.feature import Feature
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    generate_random_table_name,
)
from feathub.table.table_descriptor import TableDescriptor


class MockTableDescriptor(TableDescriptor):
    def __init__(
        self,
        name: str = generate_random_table_name("mock_descriptor"),
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
    ) -> None:
        super().__init__(
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    def get_output_features(self) -> List[Feature]:
        raise RuntimeError("Unsupported operation.")

    def is_bounded(self) -> bool:
        raise RuntimeError("Unsupported operation.")

    def get_bounded_view(self) -> TableDescriptor:
        raise RuntimeError("Unsupported operation.")

    def to_json(self) -> Dict:
        raise RuntimeError("Unsupported operation.")

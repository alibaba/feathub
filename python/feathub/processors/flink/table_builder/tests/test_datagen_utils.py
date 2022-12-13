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

from feathub.common.types import Int32, Timestamp
from feathub.feature_tables.sources.datagen_source import (
    DataGenSource,
    RandomField,
    SequenceField,
)
from feathub.processors.flink.table_builder.datagen_utils import (
    get_table_from_data_gen_source,
)
from feathub.processors.flink.table_builder.tests.table_builder_test_utils import (
    FlinkTableBuilderTestBase,
)

from feathub.table.schema import Schema


class DataGenUtilsTest(FlinkTableBuilderTestBase):
    def test_data_gen_source(self):
        source = DataGenSource(
            name="datagen_src",
            rows_per_second=10,
            field_configs={
                "id": SequenceField(start=0, end=9),
                "val": RandomField(minimum=0, maximum=100),
            },
            schema=Schema(["id", "val", "ts"], [Int32, Int32, Timestamp]),
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        table = get_table_from_data_gen_source(self.t_env, source)
        df = table.to_pandas()
        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["val"] >= 0).all() and (df["val"] <= 100).all())

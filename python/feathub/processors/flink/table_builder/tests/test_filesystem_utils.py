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
from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.processors.flink.table_builder.source_sink_utils import insert_into_sink
from feathub.processors.flink.table_builder.tests.table_builder_test_utils import (
    FlinkTableBuilderTestBase,
    MockTableDescriptor,
)
from feathub.table.table_descriptor import TableDescriptor


class SinkUtilTest(FlinkTableBuilderTestBase):
    def test_unsupported_file_format(self):
        sink = FileSystemSink("s3://dummy-bucket/path", "csv")
        table = self.t_env.from_elements([(1,)])
        with self.assertRaisesRegex(
            FeathubException, "Cannot sink files in CSV format to s3"
        ):
            descriptor: TableDescriptor = MockTableDescriptor(keys=["id"])
            insert_into_sink(self.t_env, table, descriptor, sink)

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

from feathub.processors.flink.table_builder.print_utils import insert_into_print_sink
from feathub.processors.flink.table_builder.tests.table_builder_test_utils import (
    FlinkTableBuilderTestBase,
)


class PrintSinkTest(FlinkTableBuilderTestBase):
    def test_print_sink(self):
        table = self.t_env.from_elements([(1,), (2,)], ["val"])

        table_result = insert_into_print_sink(self.t_env, table)
        table_result.wait()

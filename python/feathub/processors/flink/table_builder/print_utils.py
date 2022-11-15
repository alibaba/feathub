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
from pyflink.table import (
    TableResult,
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
)

from feathub.processors.flink.table_builder.source_sink_utils_common import (
    generate_random_table_name,
    get_schema_from_table,
)


def insert_into_print_sink(
    t_env: StreamTableEnvironment, table: NativeFlinkTable
) -> TableResult:

    # TODO: Alibaba Cloud Realtime Compute has bug that assumes all the tables should
    # have a name in VVR-6.0.2, which should be fixed in next version VVR-6.0.3. As a
    # current workaround, we have to generate a random table name. We should update the
    # code to use anonymous table sink after VVR-6.0.3 is released.
    random_sink_name = generate_random_table_name("PrintSink")
    t_env.create_temporary_table(
        random_sink_name,
        NativeFlinkTableDescriptor.for_connector("print")
        .schema(get_schema_from_table(table))
        .build(),
    )
    return table.execute_insert(random_sink_name)

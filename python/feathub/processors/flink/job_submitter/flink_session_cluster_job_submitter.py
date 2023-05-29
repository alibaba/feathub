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
from __future__ import annotations

import typing
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Optional, Dict, Sequence

from py4j.protocol import Py4JJavaError
from pyflink.table import TableResult

from feathub.common.exceptions import FeathubException
from feathub.common.utils import get_table_schema
from feathub.feature_tables.sinks.memory_store_sink import MemoryStoreSink
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.flink.flink_table import FlinkTable
from feathub.processors.flink.job_submitter.flink_job_submitter import FlinkJobSubmitter
from feathub.processors.flink.table_builder.source_sink_utils import (
    add_sink_to_statement_set,
)
from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.processors.processor_job import ProcessorJob, Future
from feathub.table.table_descriptor import TableDescriptor

if typing.TYPE_CHECKING:
    from feathub.processors.flink.flink_processor import FlinkProcessor


class FlinkSessionClusterJobSubmitter(FlinkJobSubmitter):
    """The Flink job submitter for session cluster mode."""

    def __init__(self, flink_processor: "FlinkProcessor"):
        """
        Instantiate the FlinkSessionClusterJobSubmitter.

        :param flink_processor: The FlinkProcessor that instantiate this
                                FlinkSessionClusterJobSubmitter.
        """
        self.flink_processor = flink_processor
        self._executor = ThreadPoolExecutor()

    def submit(
        self,
        materialization_descriptors: Sequence[MaterializationDescriptor],
        local_registry_tables: Dict[str, TableDescriptor],
    ) -> ProcessorJob:
        flink_table_builder = self.flink_processor.flink_table_builder
        with flink_table_builder.class_loader:
            statement_set = None
            for materialization_descriptor in materialization_descriptors:
                feature_descriptor = materialization_descriptor.feature_descriptor
                if not isinstance(feature_descriptor, TableDescriptor):
                    raise FeathubException(
                        f"The FeatureDescriptor is unresolved: {feature_descriptor}."
                    )
                table = FlinkTable(
                    flink_processor=self.flink_processor,
                    feature=feature_descriptor,
                    keys=None,
                    start_datetime=materialization_descriptor.start_datetime,
                    end_datetime=materialization_descriptor.end_datetime,
                )

                if isinstance(materialization_descriptor.sink, MemoryStoreSink):
                    if feature_descriptor.keys is None:
                        raise FeathubException(
                            f"Features keys must not be None " f"{feature_descriptor}."
                        )
                    MemoryOnlineStore.get_instance().put(
                        table_name=materialization_descriptor.sink.table_name,
                        features=table.to_pandas(),
                        schema=get_table_schema(feature_descriptor),
                        key_fields=feature_descriptor.keys,
                        timestamp_field=feature_descriptor.timestamp_field,
                        timestamp_format=feature_descriptor.timestamp_format,
                    )
                    continue

                if statement_set is None:
                    statement_set = flink_table_builder.t_env.create_statement_set()

                native_flink_table = flink_table_builder.build(
                    features=feature_descriptor,
                    keys=None,
                    start_datetime=materialization_descriptor.start_datetime,
                    end_datetime=materialization_descriptor.end_datetime,
                    clear_built_tables=False,
                )

                add_sink_to_statement_set(
                    flink_table_builder.t_env,
                    statement_set,
                    native_flink_table,
                    feature_descriptor,
                    materialization_descriptor.sink,
                )

            flink_table_builder.clear_built_tables()

            if statement_set is None:
                return FlinkSessionClusterJob(None, self._executor)

            return FlinkSessionClusterJob(statement_set.execute(), self._executor)


class FlinkSessionClusterJob(ProcessorJob):
    """Represent a Flink job that runs in Flink session cluster."""

    def __init__(self, table_result: Optional[TableResult], executor: Executor) -> None:
        """
        Instantiate a FlinkSessionClusterJob.

        :param table_result: The Flink TableResult that is typically returned when
                             executing a Flink Table job. If it is None, it means the
                             job is already finished.
        :param executor: The executor to run async task.

        """
        super().__init__()
        self._table_result = table_result
        self._executor = executor

    def cancel(self) -> Future:
        if self._executor is None:
            f: Future[None] = Future()
            f.set_result(None)
            return f

        return self._executor.submit(self._cancel)

    def _cancel(self) -> None:
        return self._table_result.get_job_client().cancel().result()

    def wait(self, timeout_ms: Optional[int] = None) -> None:
        if self._table_result is None:
            return
        try:
            self._table_result.wait(timeout_ms)
        except Py4JJavaError as e:
            if self._is_job_cancellation_exception(e):
                return
            raise e

    @staticmethod
    def _is_job_cancellation_exception(e: Py4JJavaError) -> bool:
        java_exception = e.java_exception
        while java_exception is not None:
            if "JobCancellationException" in java_exception.getClass().getName():
                return True
            java_exception = java_exception.getCause()
        return False

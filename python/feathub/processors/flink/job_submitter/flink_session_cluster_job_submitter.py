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
from __future__ import annotations

import typing
from concurrent.futures import Executor, ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Dict, Union

import pandas as pd
from py4j.protocol import Py4JJavaError
from pyflink.table import TableResult

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.online_store_sink import OnlineStoreSink
from feathub.online_stores.online_store import OnlineStore
from feathub.processors.flink.flink_table import FlinkTable
from feathub.processors.flink.job_submitter.flink_job_submitter import FlinkJobSubmitter
from feathub.processors.flink.table_builder.source_sink_utils import insert_into_sink
from feathub.processors.processor_job import ProcessorJob, Future
from feathub.table.table_descriptor import TableDescriptor

if typing.TYPE_CHECKING:
    from feathub.processors.flink.flink_processor import FlinkProcessor


class FlinkSessionClusterJobSubmitter(FlinkJobSubmitter):
    """The Flink job submitter for session cluster mode."""

    def __init__(
        self, flink_processor: "FlinkProcessor", stores: Dict[str, OnlineStore]
    ):
        """
        Instantiate the FlinkSessionClusterJobSubmitter.

        :param flink_processor: The FlinkProcessor that instantiate this
                                FlinkSessionClusterJobSubmitter.
        :param stores: A dict that maps each store type to an online store.
        """
        self.flink_processor = flink_processor
        self.stores = stores
        self._executor = ThreadPoolExecutor()

    def submit(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
        sink: FeatureTable,
        local_registry_tables: Dict[str, TableDescriptor],
        allow_overwrite: bool,
    ) -> ProcessorJob:
        table = FlinkTable(
            flink_processor=self.flink_processor,
            feature=features,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        if isinstance(sink, OnlineStoreSink):
            if features.keys is None:
                raise FeathubException(f"Features keys must not be None {features}.")
            self.stores[sink.store_type].put(
                table_name=sink.table_name,
                features=table.to_pandas(),
                key_fields=features.keys,
                timestamp_field=features.timestamp_field,
                timestamp_format=features.timestamp_format,
            )
            return FlinkSessionClusterJob(None, self._executor)

        native_flink_table = self.flink_processor.flink_table_builder.build(
            features=features,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

        table_result = insert_into_sink(
            self.flink_processor.flink_table_builder.t_env,
            native_flink_table,
            sink,
            features.keys,
        )
        return FlinkSessionClusterJob(table_result, self._executor)


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

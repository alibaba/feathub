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
from concurrent.futures import Future
from typing import Optional

from pyflink.table import TableResult

from feathub.processors.processor_job import ProcessorJob


class FlinkSessionClusterJob(ProcessorJob):
    """Represent a Flink job that runs in Flink session cluster."""

    def __init__(self, table_result: Optional[TableResult]) -> None:
        """
        Instantiate a FlinkSessionClusterJob.

        :param table_result: The Flink TableResult that is typically returned when
                             executing a Flink Table job. If it is None, it means the
                             job is already finished.

        """
        super().__init__()
        self._table_result = table_result

    def wait(self, timeout_ms: Optional[int] = None) -> None:
        if self._table_result is None:
            return

        self._table_result.wait(timeout_ms)


class FlinkApplicationClusterJob(ProcessorJob):
    """Represent a Flink job that runs in Application cluster."""

    def __init__(self, job_future: Future):
        """
        Instantiate a FlinkApplicationClusterJob.

        :param job_future: A Future object which is done when the application Flink job
                           reaches global termination state.
        """
        super().__init__()
        self._job_future = job_future

    def wait(self, timeout_ms: Optional[int] = None) -> None:
        timeout_sec = None if timeout_ms is None else timeout_ms / 1000
        self._job_future.result(timeout=timeout_sec)

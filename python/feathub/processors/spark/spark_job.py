# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from concurrent.futures import Future
from typing import Optional, Callable

from feathub.processors.processor_job import ProcessorJob


class SparkJob(ProcessorJob):
    """Represent a Spark job."""

    def __init__(
        self,
        job_future: Future,
    ) -> None:
        super().__init__()
        self._job_future = job_future

    # TODO: Add test case to verify this method's behavior when job future
    #  is completed exceptionally.
    def cancel(self) -> Future:
        cancel_future: Future = Future()
        job_future_callback = self._get_job_future_callback(cancel_future)
        self._job_future.add_done_callback(job_future_callback)
        return cancel_future

    def wait(self, timeout_ms: Optional[int] = None) -> None:
        timeout_sec = None if timeout_ms is None else timeout_ms / 1000
        self._job_future.result(timeout=timeout_sec)

    @staticmethod
    def _get_job_future_callback(cancel_future: Future) -> Callable[[Future], None]:
        def job_future_callback(job_future: Future) -> None:
            if job_future.cancelled() or job_future.exception() is None:
                cancel_future.set_result(None)
            else:
                cancel_future.set_exception(job_future.exception())

        return job_future_callback

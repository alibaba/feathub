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
from datetime import timedelta
from typing import Dict

from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.sinks.sink import Sink


class PrometheusSink(Sink):
    def __init__(
        self,
        server_url: str,
        job_name: str,
        delete_on_shutdown: bool,
        extra_labels: Dict[str, str],
        retry_timeout: timedelta,
    ):
        """
        :param server_url: The PushGateway server server URL including scheme,
                         host name, and port.
        :param job_name: The value of the `job` label of the metrics emitted by
                         this sink.
        :param delete_on_shutdown: Whether to delete metrics from Prometheus
                                   when the job finishes. When set to true,
                                   Feathub will try its best to delete the
                                   metrics but this is not guaranteed.
        :param extra_labels: The extra labels of all metrics pushed by this sink.
        :param retry_timeout: The max timeout this sink may take retrying in case
                              of occasional exceptions.
        """
        super().__init__(
            name="",
            system_name="prometheus",
            table_uri={
                "server_url": server_url,
            },
        )
        self.server_url = server_url
        self.job_name = job_name
        self.delete_on_shutdown = delete_on_shutdown
        self.extra_labels = extra_labels
        self.retry_timeout = retry_timeout

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "server_url": self.server_url,
            "job_name": self.job_name,
            "delete_on_shutdown": self.delete_on_shutdown,
            "extra_labels": self.extra_labels,
            "retry_timeout_ms": self.retry_timeout / timedelta(milliseconds=1),
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PrometheusSink":
        return PrometheusSink(
            server_url=json_dict["server_url"],
            job_name=json_dict["job_name"],
            delete_on_shutdown=json_dict["delete_on_shutdown"],
            extra_labels=json_dict["extra_labels"],
            retry_timeout=timedelta(milliseconds=json_dict["retry_timeout_ms"]),
        )

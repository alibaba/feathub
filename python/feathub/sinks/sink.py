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

from abc import ABC, abstractmethod


class Sink(ABC):
    """
    Provides metadata to locate write a table of feature values to an offline or online
    feature store.
    """

    def __init__(
        self,
        store_type: str,
        allow_overwrite: bool,
    ):
        """
        :param store_type: A string that uniquely identifies a store class.
        :param allow_overwrite: True if rows of tables in this store can be overwritten.
        """
        super().__init__()
        self.store_type = store_type
        self.allow_overwrite = allow_overwrite

    @abstractmethod
    def to_json(self):
        """
        Returns a json-formatted object representing this sink.
        """
        pass

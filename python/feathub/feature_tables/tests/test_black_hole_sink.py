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
from abc import ABC

from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class BlackHoleSinkITTest(ABC, FeathubITTestBase):
    def test_black_hole_sink(self):
        source = self.create_file_source(self.input_data.copy())

        sink = BlackHoleSink()

        self.client.materialize_features(
            feature_descriptor=source, sink=sink, allow_overwrite=True
        ).wait()

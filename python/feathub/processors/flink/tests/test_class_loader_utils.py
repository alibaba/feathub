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
import unittest

from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray

from feathub.processors.flink.flink_class_loader_utils import (
    ClassLoader,
    get_flink_context_class_loader,
    set_flink_context_class_loader,
)


class FlinkClassLoaderUtilsTest(unittest.TestCase):
    def test_class_loader(self):
        class_loader = get_flink_context_class_loader()

        class_loader_1 = self._create_class_loader(class_loader)
        class_loader_2 = self._create_class_loader(class_loader)

        set_flink_context_class_loader(class_loader_2)

        self.assertEquals(class_loader_2, get_flink_context_class_loader())

        with class_loader_1:
            self.assertEquals(class_loader_1, get_flink_context_class_loader())
            with class_loader_2:
                self.assertEquals(class_loader_2, get_flink_context_class_loader())
                with class_loader_1:
                    self.assertEquals(class_loader_1, get_flink_context_class_loader())

        self.assertEquals(class_loader_2, get_flink_context_class_loader())

    def _create_class_loader(self, parent_class_loader: ClassLoader) -> ClassLoader:
        gateway = get_gateway()
        url_class_loader = gateway.jvm.java.net.URLClassLoader(
            to_jarray(gateway.jvm.java.net.URL, []), parent_class_loader.j_class_loader
        )
        return ClassLoader(url_class_loader)

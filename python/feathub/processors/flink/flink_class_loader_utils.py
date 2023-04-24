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
from typing import Any, List

from pyflink.java_gateway import get_gateway


class ClassLoader:
    """
    The class loader. You can use the with statement to run a block of code with a
    class loader.
    """

    def __init__(self, j_class_loader: Any):
        self.j_class_loader = j_class_loader
        self._prev_class_loader: List[ClassLoader] = []

    def __enter__(self) -> "ClassLoader":
        self._prev_class_loader.append(get_flink_context_class_loader())
        set_flink_context_class_loader(self)
        return self

    def __exit__(self, *args: Any) -> None:
        set_flink_context_class_loader(self._prev_class_loader.pop())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ClassLoader):
            return False

        return self.j_class_loader.equals(other.j_class_loader)

    def __str__(self) -> str:
        return self.j_class_loader.toString()

    def __repr__(self) -> str:
        return self.__str__()


def get_flink_context_class_loader() -> ClassLoader:
    """
    Return the current context class loader of Flink.
    """
    return ClassLoader(get_gateway().jvm.Thread.currentThread().getContextClassLoader())


def set_flink_context_class_loader(class_loader: ClassLoader) -> None:
    """
    Set the current context class loader of Flink.
    """

    get_gateway().jvm.Thread.currentThread().setContextClassLoader(
        class_loader.j_class_loader
    )

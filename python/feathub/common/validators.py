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
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Iterable

from feathub.common.exceptions import FeathubConfigurationException

T = TypeVar("T")


class Validator(ABC, Generic[T]):
    """
    Validator is used to perform single configuration validation.
    """

    @abstractmethod
    def ensure_valid(self, name: str, value: T) -> None:
        """
        Perform single configuration validation.

        :param name: The name of the configuration
        :param value: The value of the configuration

        :raise FeathubConfigurationException if the value is invalid.
        """
        pass


class NotNoneValidator(Validator[T]):
    def ensure_valid(self, name: str, value: T) -> None:
        if value is None:
            raise FeathubConfigurationException(f"Value of {name} cannot be None.")


def not_none() -> Validator[T]:
    return NotNoneValidator()


class InListValidator(Validator[T]):
    def __init__(self, *allowed: T):
        self.allowed = allowed

    def ensure_valid(self, name: str, value: T) -> None:
        if value not in self.allowed:
            raise FeathubConfigurationException(
                f"Invalid value {value} of {name}: Value must be one of "
                f"{', '.join([str(v) for v in self.allowed])}."
            )


def in_list(*allowed: T) -> Validator[T]:
    """
    Returns a validator check if the value is in the given list of allowed values.

    :param allowed: Allowed values.
    """
    return InListValidator(*allowed)


ITER_T = TypeVar("ITER_T", bound=Iterable)


class IsSubSetValidator(Validator[ITER_T]):
    def __init__(self, *allowed: T):
        self.in_set_validator = InListValidator(*allowed)

    def ensure_valid(self, name: str, value: Iterable[T]) -> None:
        for v in value:
            self.in_set_validator.ensure_valid(name, v)


def is_subset(*allowed: T) -> Validator[ITER_T]:
    """
    Returns a validator check if the elements in a collection typed value are in the
    set of allowed values.

    :param allowed: Allowed values.
    """
    return IsSubSetValidator(*allowed)

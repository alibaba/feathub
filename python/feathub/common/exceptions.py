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


class FeathubException(Exception):
    """
    Base exception for FeatHub.
    """


class FeathubConfigurationException(FeathubException):
    """
    Raised when there is FeatHub configuration problem.
    """


class FeathubTypeException(FeathubException):
    """
    Raised when there is FeatHub type problem.
    """


class FeathubTransformationException(FeathubException):
    """
    Raised when there is FeatHub transformation problem.
    """


class FeathubExpressionException(FeathubException):
    """
    Raised when there is problem of FeatHub expression.
    """

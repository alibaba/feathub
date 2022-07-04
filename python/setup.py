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
import os
import sys

from setuptools import setup, find_packages

this_directory = os.path.abspath(os.path.dirname(__file__))
version_file = os.path.join(this_directory, "feathub/version.py")

try:
    exec(open(version_file).read())
except IOError:
    print(
        "Failed to load Feathub version file for packaging. "
        + "'%s' not found!" % version_file,
        file=sys.stderr,
    )
    sys.exit(-1)
VERSION = __version__  # noqa

PACKAGES = find_packages(include=["feathub", "feathub.*"], exclude=["*tests*"])

install_requires = [
    "sklearn",
    "ply>=3.11",
    "pandas>=1.1.5",
    "numpy>=1.14.3,<1.20",
    "apache-flink~=1.15",
    "kubernetes~=24.2",
]

setup(
    name="feathub",
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    license="https://www.apache.org/licenses/LICENSE-2.0",
    author="Apache Software Foundation",
    python_requires=">=3.6",
    install_requires=install_requires,
    tests_require=["pytest==4.4.1"],
    zip_safe=True,
)

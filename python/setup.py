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
from shutil import rmtree, copytree

from setuptools import setup, find_packages


def remove_if_exists(file_path):
    if os.path.exists(file_path):
        if os.path.islink(file_path) or os.path.isfile(file_path):
            os.remove(file_path)
        else:
            assert os.path.isdir(file_path)
            rmtree(file_path)


# clear setup cache directories
remove_if_exists("build")

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

TEMP_PATH = "deps"
TEMP_FLINK_PROCESSOR_LIB_DIR = os.path.join(TEMP_PATH, "processors", "flink", "lib")

feathub_version = VERSION.replace(".dev0", "-SNAPSHOT")
FEATHUB_FLINK_PROCESSOR_LIB_DIR = os.path.join(
    this_directory,
    "..",
    "java",
    "feathub-dist",
    "target",
    f"feathub-dist-{feathub_version}-bin",
    f"feathub-dist-{feathub_version}",
    "lib",
)
in_feathub_source = os.path.isfile("../java/feathub-dist/pom.xml")

try:
    if in_feathub_source:
        try:
            os.mkdir(TEMP_PATH)
        except:
            print(
                "Temp path for symlink to parent already exists {0}".format(TEMP_PATH),
                file=sys.stderr,
            )
            sys.exit(-1)

        try:
            os.symlink(FEATHUB_FLINK_PROCESSOR_LIB_DIR, TEMP_FLINK_PROCESSOR_LIB_DIR)
        except BaseException:  # pylint: disable=broad-except
            copytree(FEATHUB_FLINK_PROCESSOR_LIB_DIR, TEMP_FLINK_PROCESSOR_LIB_DIR)

    PACKAGES = find_packages(include=["feathub", "feathub.*"], exclude=["*tests*"])
    PACKAGES.append("feathub.processors.flink.lib")

    PACKAGE_DIR = {
        "feathub.processors.flink.lib": TEMP_FLINK_PROCESSOR_LIB_DIR,
    }

    PACKAGE_DATA = {
        "feathub.processors.flink.lib": ["*.jar"],
    }

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
        package_dir=PACKAGE_DIR,
        package_data=PACKAGE_DATA,
        license="https://www.apache.org/licenses/LICENSE-2.0",
        author="Feathub Authors",
        python_requires=">=3.6",
        install_requires=install_requires,
        tests_require=["pytest==4.4.1"],
        zip_safe=True,
    )
finally:
    if in_feathub_source:
        remove_if_exists(TEMP_PATH)

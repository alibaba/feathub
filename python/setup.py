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
import os
import subprocess
import sys
from datetime import datetime
from shutil import rmtree, copytree, which

from setuptools import setup, find_packages
from setuptools.command.build import build


def remove_if_exists(file_path):
    if os.path.exists(file_path):
        if os.path.islink(file_path) or os.path.isfile(file_path):
            os.remove(file_path)
        else:
            assert os.path.isdir(file_path)
            rmtree(file_path)


class BuildCommand(build):
    def run(self):
        self.run_command("generate_py_protobufs")
        return super().run()


# check protoc command and version
if not which("protoc"):
    print(
        "Command 'protoc' not found. Please make sure protoc 3.17 "
        "is installed in the local environment."
    )
    sys.exit(-1)

protoc_version = subprocess.check_output(
    ["protoc", "--version"], stderr=subprocess.STDOUT
)
if not protoc_version.startswith(b"libprotoc 3.17"):
    print(
        f"protoc '{protoc_version}' is installed in the local environment, "
        f"while FeatHub has only been verified with protoc 3.17.x."
    )
    sys.exit(-1)

# clear setup cache directories
remove_if_exists("build")

this_directory = os.path.abspath(os.path.dirname(__file__))
nightly_build = os.getenv("NIGHTLY_BUILD") == "true"
version_file = os.path.join(this_directory, "feathub/version.py")

try:
    exec(open(version_file).read())
except IOError:
    print(
        "Failed to load FeatHub version file for packaging. "
        + "'%s' not found!" % version_file,
        file=sys.stderr,
    )
    sys.exit(-1)

PACKAGE_NAME = "feathub"
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

if nightly_build:
    if "dev" not in VERSION:
        raise RuntimeError("Nightly wheel is not supported for non dev version")
    VERSION = VERSION[: str.find(VERSION, "dev") + 3] + datetime.now().strftime(
        "%Y%m%d"
    )
    PACKAGE_NAME = f"{PACKAGE_NAME}_nightly"


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
        "feathub.examples": ["*.csv"],
    }

    install_requires = [
        "scikit-learn",
        "ply>=3.11",
        "pandas>=1.1.5",
        "numpy~=1.21.4",
        "kubernetes~=24.2",
        "protobuf~=3.20.0",
        "python-dateutil~=2.8",
        "redis==4.3.0",
        "tzlocal~=4.2",
        "mysql-connector-python~=8.0.0",
    ]

    extras_require = {
        "flink": [
            "apache-flink==1.16.1",
        ],
        "spark": [
            "pyspark==3.3.1",
        ],
    }

    setup(
        name=PACKAGE_NAME,
        version=VERSION,
        description="A stream-batch unified feature store for real-time machine "
        "learning",
        url="https://github.com/alibaba/feathub",
        packages=PACKAGES,
        include_package_data=True,
        package_dir=PACKAGE_DIR,
        package_data=PACKAGE_DATA,
        setup_requires=["protobuf_distutils"],
        cmdclass={
            "build": BuildCommand,
        },
        options={
            "generate_py_protobufs": {
                "source_dir": os.path.join(this_directory, "feathub/common/protobuf"),
                "output_dir": os.path.join(this_directory, "feathub/common/protobuf"),
            },
        },
        license="https://www.apache.org/licenses/LICENSE-2.0",
        author="FeatHub Authors",
        python_requires=">=3.6",
        install_requires=install_requires,
        extras_require=extras_require,
        tests_require=["pytest==4.4.1"],
        zip_safe=True,
    )
finally:
    if in_feathub_source:
        remove_if_exists(TEMP_PATH)

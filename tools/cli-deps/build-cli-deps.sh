#
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
#

# This script builds a zip that includes all the python dependencies to run a feathub
# job when submitting with Flink cli.

set -e

CURRENT_DIR=$(dirname "${BASH_SOURCE-$0}")
CURRENT_DIR=$(cd "${CURRENT_DIR}"; pwd)
PROJECT_DIR=$(cd "${CURRENT_DIR}/../.."; pwd)
PYTHON_DIR=${PROJECT_DIR}/python
JAVA_DIR=${PROJECT_DIR}/java

# build java dependencies
cd "${JAVA_DIR}"
mvn clean package -DskipTests

# build wheels
cd "${PYTHON_DIR}"
WITHOUT_PYFLINK=1 python3 setup.py bdist_wheel
[ -d "${CURRENT_DIR}"/wheels ] && rm -rf "${CURRENT_DIR}"/wheels
mkdir "${CURRENT_DIR}"/wheels
cp dist/feathub-*.whl "${CURRENT_DIR}"/wheels

docker run -it --rm -v "${CURRENT_DIR}":/build -w /build quay.io/pypa/manylinux2014_x86_64 ./build.sh

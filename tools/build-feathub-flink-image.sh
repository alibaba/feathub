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

# The script build the docker image for FlinkProcessor to run the Flink job in
# Kubernetes application mode.

set -e

CURRENT_DIR=$(dirname "${BASH_SOURCE-$0}")
PROJECT_DIR=$(cd "${CURRENT_DIR}/.."; pwd)
DOCKER_DIR=${PROJECT_DIR}/docker
PYTHON_DIR=${PROJECT_DIR}/python
JAVA_DIR=${PROJECT_DIR}/java

# build java dependencies
cd ${JAVA_DIR}
mvn clean package -DskipTests

# build wheels
cd ${PYTHON_DIR}
python3 setup.py bdist_wheel
[ -d ${DOCKER_DIR}/wheels ] && rm -rf ${DOCKER_DIR}/wheels
mkdir ${DOCKER_DIR}/wheels
cp dist/feathub-*.whl ${DOCKER_DIR}/wheels

# build docker image
cd ${DOCKER_DIR}
docker build --tag feathub:latest .
rm -rf ${DOCKER_DIR}/wheels
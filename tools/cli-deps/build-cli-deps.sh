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

# zip Feathub and its dependencies
cd "${CURRENT_DIR}"
# TODO: Install the latest stable version after Feathub released.
python -m pip install --target __pypackages__ feathub-nightly --no-deps
python -m pip install --target __pypackages__ -r requirements.txt
cd __pypackages__ && zip -r deps.zip . && mv deps.zip ../ && cd ..
rm -rf __pypackages__
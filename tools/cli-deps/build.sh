#!/bin/bash
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

# Install the packages in the ./wheels folder as well as packages specified in the
# requirements.txt. Then zip the installed package.

set -e -x
yum install -y zip

PYBIN=/opt/python/cp37-cp37m/bin

"${PYBIN}/pip" install --target __pypackages__ wheels/* --no-deps
"${PYBIN}/pip" install --target __pypackages__ -r requirements.txt
cd __pypackages__ && zip -r deps.zip . && mv deps.zip ../ && cd ..
rm -rf __pypackages__

#!/usr/bin/env bash

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

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

##
## Required variables
##
RELEASE_VERSION=${RELEASE_VERSION}

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

RELEASE_DIR=${BASE_DIR}/release/
mkdir -p ${RELEASE_DIR}

cd ${PROJECT_ROOT}/java/
${MVN} clean package -B -DskipTests

python -m pip install --upgrade pip setuptools wheel
cd ${PROJECT_ROOT}/python/

# create a target/ directory like in MAVEN.
# this directory will contain a temporary copy of the source
# eventually the built artifact will be copied to ${PROJECT_ROOT}/python/dist and this target
# directory will be deleted.
rm -rf dist/
rm -rf ../target/
mkdir -p ../target/

# copy all the sources into target
rsync -a * ../target/

cd ../target/

python3 setup.py bdist_wheel

cp -r dist ${PROJECT_ROOT}/python/dist

mv dist/* ${RELEASE_DIR}

cd ..
rm -rf target

SOURCE_DIST="feathub-$RELEASE_VERSION-py3-none-any.whl"

cd ${RELEASE_DIR}
${SHASUM} "${SOURCE_DIST}" > "${SOURCE_DIST}.sha512"

cd ${CURR_DIR}

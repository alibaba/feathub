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
## Required variables
##
TAG=${TAG}
GITHUB_TOKEN=${GITHUB_TOKEN}
SHORT_RELEASE_VERSION=${SHORT_RELEASE_VERSION}

if [ -z "${TAG}" ]; then
    echo "TAG was not set."
    exit 1
fi

if [ -z "${GITHUB_TOKEN}" ]; then
    echo "GITHUB_TOKEN was not set."
    exit 1
fi

if [ -z "${SHORT_RELEASE_VERSION}" ]; then
    echo "SHORT_RELEASE_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../.."

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

cd $PROJECT_ROOT

GIT_REMOTE_NAME=`git remote -v | grep "alibaba/feathub" | grep push | cut -d$'\t' -f1`

if [[ -z "$GIT_REMOTE_NAME" ]]; then
    echo "alibaba/feathub is not configured as a remote repository for this directory."
    exit 1
fi

if ! git ls-remote --tags $GIT_REMOTE_NAME | grep -q "${TAG}"; then
    echo "Tag ${TAG} not found in remote repository. Make sure you have pushed the tag before running this script."
    exit 1
fi

release_id=`
    curl -L \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        https://api.github.com/repos/alibaba/feathub/releases \
        -d "{\"tag_name\":\"${TAG}\",\"target_commitish\":\"release-${SHORT_RELEASE_VERSION}\",\"name\":\"${TAG}\",\"body\":\"${TAG}\",\"draft\":false,\"prerelease\":true,\"generate_release_notes\":true}" \
    | grep -o 'releases/\d\+\"' \
    | grep -o '\d\+'
`

for filename in ${BASE_DIR}/release/*; do
    base_name=$(basename ${filename})
    
    curl -L \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        -H "Content-Type: application/octet-stream" \
        "https://uploads.github.com/repos/alibaba/feathub/releases/${release_id}/assets?name=${base_name}" \
        --data-binary "@${filename}"
done

cd ${CURR_DIR}

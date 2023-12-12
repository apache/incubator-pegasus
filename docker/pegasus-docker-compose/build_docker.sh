#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#!/bin/bash

set -e

if [[ $# -lt 1 ]]; then
    echo "USAGE: $0 /your/local/apache-pegasus-source [github-branch-for-build-env-image(default: master)]"
    exit 1
fi

# ROOT is where the script is.
ROOT=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")

# PEGASUS_ROOT is the absolute path of apache-pegasus-source
PEGASUS_ROOT="$(readlink -f "$1")"
if [[ ! -f "${PEGASUS_ROOT}"/PACKAGE ]]; then
    echo "ERROR: no such file ${PEGASUS_ROOT}/PACKAGE"
    exit 1
fi
SERVER_PKG_NAME=$(cat "${PEGASUS_ROOT}"/PACKAGE)
if [[ ! -f "${PEGASUS_ROOT}/${SERVER_PKG_NAME}.tar.gz" ]]; then
    echo "Failed to find package ${SERVER_PKG_NAME}.tar.gz in ${PEGASUS_ROOT}"
    exit 1
else
    echo "Found package ${PEGASUS_ROOT}/${SERVER_PKG_NAME}.tar.gz"
fi

GITHUB_BRANCH=master
if [[ $# -ge 2 ]]; then
    GITHUB_BRANCH=$2
fi

###
# configurable
IMAGE_NAME=pegasus:latest
###

echo "Building image ${IMAGE_NAME}"
cd "${ROOT}"/image_for_prebuilt_bin || exit 1

cp "${PEGASUS_ROOT}/${SERVER_PKG_NAME}.tar.gz" .
docker build --build-arg SERVER_PKG_NAME="${SERVER_PKG_NAME}" --build-arg GITHUB_BRANCH="{GITHUB_BRANCH}" -t ${IMAGE_NAME} .
rm "${SERVER_PKG_NAME}.tar.gz"

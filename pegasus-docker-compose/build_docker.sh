#!/bin/bash

# Usage:
# 	./build_docker.sh /your/local/apache-pegasus-source

set -e

if [[ $# -ne 1 ]]; then
	echo "ERROR: must specify /your/local/apache-pegasus-source"
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

###
# configurable
IMAGE_NAME=pegasus:latest
###

echo "Building image ${IMAGE_NAME}"
cd "${ROOT}"/image_for_prebuilt_bin || exit 1

cp "${PEGASUS_ROOT}/${SERVER_PKG_NAME}.tar.gz" .
docker build --build-arg SERVER_PKG_NAME="${SERVER_PKG_NAME}" -t ${IMAGE_NAME} .
rm "${SERVER_PKG_NAME}.tar.gz"

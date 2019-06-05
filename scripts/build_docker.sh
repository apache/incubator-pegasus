#!/bin/bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
ROOT=$(
	cd "$(dirname "${SCRIPT_DIR}")" || exit 1
	pwd
)

# configurable variables
SERVER_PKG_NAME=pegasus-server-1.12.SNAPSHOT-75be7db-ubuntu-release
IMAGE_NAME=pegasus:latest

echo "Building image ${IMAGE_NAME}"
cd "${ROOT}" || exit 1

if [[ -z ${SERVER_PKG_NAME} ]]; then
	echo "SERVER_PKG_NAME is empty"
	exit 1
fi

if [[ ! -f "${ROOT}"/${SERVER_PKG_NAME}.tar.gz ]]; then
	echo "Failed to find package ${SERVER_PKG_NAME}.tar.gz in ${ROOT}"
	exit 1
else
	echo "Found package ${ROOT}/${SERVER_PKG_NAME}.tar.gz"
fi

cd "${ROOT}" || exit 1
mkdir -p ${IMAGE_NAME}
cp -f docker/dev/ubuntu18.04/Dockerfile ${IMAGE_NAME}/Dockerfile
sed -i 's/@SERVER_PKG_NAME@/'"${SERVER_PKG_NAME}"'/' ${IMAGE_NAME}/Dockerfile
cp ${SERVER_PKG_NAME}.tar.gz ${IMAGE_NAME}

cd ${IMAGE_NAME} || exit 1
docker build -t ${IMAGE_NAME} .

cd "${ROOT}" || exit 1
docker images

# clean up
rm -r ${IMAGE_NAME}

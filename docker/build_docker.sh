#!/bin/bash

set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
ROOT=$(
	cd "$(dirname "${SCRIPT_DIR}")" || exit 1
	pwd
)
SERVER_PKG_NAME=$(cat "${ROOT}"/PACKAGE)

###
# configurable variables
IMAGE_NAME=pegasus:latest
###

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
cp -f docker/dev/linux/entrypoint.sh ${IMAGE_NAME}/entrypoint.sh
cp -f docker/dev/linux/Dockerfile ${IMAGE_NAME}/Dockerfile
sed -i 's/@SERVER_PKG_NAME@/'"${SERVER_PKG_NAME}"'/' ${IMAGE_NAME}/Dockerfile
cp ${SERVER_PKG_NAME}.tar.gz ${IMAGE_NAME}

cd ${IMAGE_NAME} || exit 1
docker build --build-arg SERVER_PKG_NAME="${SERVER_PKG_NAME}" -t ${IMAGE_NAME} .

cd "${ROOT}" || exit 1
docker images

# clean up
rm -r ${IMAGE_NAME}

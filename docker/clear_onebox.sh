#!/usr/bin/env bash

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

source cluster_args.sh

if ! [[ -d "${DOCKER_DIR}" ]]; then
    echo "Cleared ${DOCKER_DIR} already"
    exit 0
fi
cd "${DOCKER_DIR}" || exit 1
pwd

docker-compose kill
docker-compose rm -f -v

cd "${ROOT}" || exit 1

./run.sh clear_zk
sudo rm -rf "${DOCKER_DIR}"

#!/usr/bin/env bash

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

source cluster_args.sh

cd "${ROOT}" || exit 1

./docker/clear_onebox.sh
./docker/prepare.sh
./run.sh start_zk

cd "${DOCKER_DIR}" || exit 1
pwd

docker-compose up -d

#!/usr/bin/env bash

set -e

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

source cluster_args.sh

cd "${ROOT}" || exit 1

"${ROOT}"/clear_onebox.sh
"${ROOT}"/prepare.sh

cd "${DOCKER_DIR}" || exit 1
pwd

docker-compose up -d

function print_nodes() {
  cd "${DOCKER_DIR}" || exit 1
  echo ""
  docker-compose ps
  echo ""
  cd - || exit 1
}

print_nodes

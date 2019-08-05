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
  echo ""
  for i in $(seq "${META_COUNT}"); do
    echo "META${i}: ${NODE_IP_PREFIX}.1$((i)):34601"
  done
  for i in $(seq "${REPLICA_COUNT}"); do
    echo "REPLICA${i}: ${NODE_IP_PREFIX}.2$((i)):34801"
  done

  cd "${DOCKER_DIR}" || exit 1
  echo ""
  docker-compose ps
  echo ""
  cd - || exit 1
}

print_nodes

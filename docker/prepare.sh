#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1
source cluster_args.sh

function print_nodes()
{
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

if ! [[ -x "$(command -v docker-compose)" ]]; then
  echo 'ERROR: docker-compose is not installed.' >&2
  echo 'See this document for installation manual:' >&2
  echo '    https://docs.docker.com/compose/install' >&2
  exit 1
fi

if [[ -d "${DOCKER_DIR}" ]]; then
    echo "ERROR: ${DOCKER_DIR} already exists, please remove it first" >&2
    print_nodes
    exit 1
fi

mkdir -p "${DOCKER_DIR}"

cp -f "${ROOT}"/src/server/config.ini "${DOCKER_DIR}/config.ini"
sed -i 's/@META_PORT@/34601/' "${DOCKER_DIR}/config.ini"
sed -i 's/@REPLICA_PORT@/34801/' "${DOCKER_DIR}/config.ini"
sed -i 's/%{cluster.name}/'"${CLUSTER_NAME}"'/g' "${DOCKER_DIR}/config.ini"
sed -i 's/%{app.dir}/\/pegasus\/data/g' "${DOCKER_DIR}/config.ini"
sed -i 's/%{slog.dir}/\/pegasus\/slog/g' "${DOCKER_DIR}/config.ini"
sed -i 's/%{data.dirs}//g' "${DOCKER_DIR}/config.ini"
sed -i 's@%{home.dir}@'"/pegasus"'@g' "${DOCKER_DIR}/config.ini"
sed -i "s/allow_non_idempotent_write = false/allow_non_idempotent_write = ${IDEMPOTENT}/" "${DOCKER_DIR}/config.ini"
for i in $(seq "${META_COUNT}"); do
    meta_port=34601
    meta_ip=${NODE_IP_PREFIX}.1$((i))
    if [ "${i}" -eq 1 ]; then
        meta_list="${meta_ip}:$meta_port"
    else
        meta_list="$meta_list,${meta_ip}:$meta_port"
    fi
done
sed -i 's/%{meta.server.list}/'"$meta_list"'/g' "${DOCKER_DIR}/config.ini"
sed -i 's/%{zk.server.list}/'"${LOCAL_IP}"':22181/g' "${DOCKER_DIR}/config.ini"
sed -i 's/app_name = .*$/app_name = '"$APP_NAME"'/' "${DOCKER_DIR}/config.ini"
sed -i 's/partition_count = .*$/partition_count = '"$PARTITION_COUNT"'/' "${DOCKER_DIR}/config.ini"

cp -f "${SCRIPT_DIR}"/docker-compose.yml "${DOCKER_DIR}"
sed -i 's/@NODE_IP_PREFIX@/'"${NODE_IP_PREFIX}"'/' "${DOCKER_DIR}"/docker-compose.yml
sed -i 's/@IMAGE_NAME@/'"${IMAGE_NAME}"'/' "${DOCKER_DIR}"/docker-compose.yml

echo "${DOCKER_DIR} is ready"
print_nodes

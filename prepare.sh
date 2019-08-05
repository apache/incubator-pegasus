#!/bin/bash

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
source cluster_args.sh

if ! [[ -x "$(command -v docker-compose)" ]]; then
    echo 'ERROR: docker-compose is not installed.' >&2
    echo 'See this document for installation manual:' >&2
    echo '    https://docs.docker.com/compose/install' >&2
    exit 1
fi

if [[ -d "${DOCKER_DIR}" ]]; then
    echo "ERROR: ${DOCKER_DIR} already exists, please remove it first" >&2
    exit 1
fi

mkdir -p "${DOCKER_DIR}"

cp -f "${ROOT}"/config.min.ini "${DOCKER_DIR}/config.ini"
sed -i "s/%{cluster.name}/${CLUSTER_NAME}/g" "${DOCKER_DIR}/config.ini"
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
zookeeper_addr=${NODE_IP_PREFIX}.31:2181
sed -i "s/%{meta.server.list}/$meta_list/g" "${DOCKER_DIR}/config.ini"
sed -i "s/%{zk.server.list}/${zookeeper_addr}/g" "${DOCKER_DIR}/config.ini"

cp -f "${ROOT}"/docker-compose.yml "${DOCKER_DIR}"
sed -i "s/@NODE_IP_PREFIX@/${NODE_IP_PREFIX}/g" "${DOCKER_DIR}"/docker-compose.yml
sed -i "s/@IMAGE_NAME@/'${IMAGE_NAME}'/g" "${DOCKER_DIR}"/docker-compose.yml

echo "${DOCKER_DIR} is ready"

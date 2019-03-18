#!/bin/bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
ROOT=$(
	cd "$(dirname "${SCRIPT_DIR}")" || exit 1
	pwd
)
LOCAL_IP=$(scripts/get_local_ip)

# Configure the following variables to customize the docker cluster. #

PARTITION_COUNT=8
APP_NAME=temp

# The ip prefix for each nodes.
# Meta-x's ip address is 172.21.0.1{x}:34601.
# Replica-x's ip address is 172.21.0.2{x}:34801
NODE_IP_PREFIX=172.21.0

CLUSTER_NAME=onebox2

IMAGE_NAME=pegasus:latest

# Config ends #

DOCKER_DIR=${CLUSTER_NAME}-docker # Where docker onebox resides.
META_COUNT=2 # Number of meta instances.
REPLICA_COUNT=5 # Number of replica instances.

if [ -d ${DOCKER_DIR} ]; then
    echo "ERROR: ${DOCKER_DIR} already exists, please remove it first" >&2
    exit 1
fi

mkdir -p ${DOCKER_DIR}

cp -f "${ROOT}"/src/server/config.ini ${DOCKER_DIR}/config.ini
sed -i 's/@META_PORT@/34601/' ${DOCKER_DIR}/config.ini
sed -i 's/@REPLICA_PORT@/34801/' ${DOCKER_DIR}/config.ini
sed -i 's/%{cluster.name}/'"${CLUSTER_NAME}"'/g' ${DOCKER_DIR}/config.ini
sed -i 's/%{app.dir}/\/pegasus\/data/g' ${DOCKER_DIR}/config.ini
sed -i 's/%{slog.dir}/\/pegasus\/slog/g' ${DOCKER_DIR}/config.ini
sed -i 's/%{data.dirs}//g' ${DOCKER_DIR}/config.ini
sed -i 's@%{home.dir}@'"/pegasus"'@g' ${DOCKER_DIR}/config.ini
for i in $(seq ${META_COUNT}); do
    meta_port=34601
    meta_ip=${NODE_IP_PREFIX}.1$((i))
    if [ "${i}" -eq 1 ]; then
        meta_list="${meta_ip}:$meta_port"
    else
        meta_list="$meta_list,${meta_ip}:$meta_port"
    fi
done
sed -i 's/%{meta.server.list}/'"$meta_list"'/g' ${DOCKER_DIR}/config.ini
sed -i 's/%{zk.server.list}/'"${LOCAL_IP}"':22181/g' ${DOCKER_DIR}/config.ini
sed -i 's/app_name = .*$/app_name = '"$APP_NAME"'/' ${DOCKER_DIR}/config.ini
sed -i 's/partition_count = .*$/partition_count = '"$PARTITION_COUNT"'/' ${DOCKER_DIR}/config.ini

cp -f "${SCRIPT_DIR}"/docker-compose.yml ${DOCKER_DIR}
sed -i 's/@NODE_IP_PREFIX@/'"${NODE_IP_PREFIX}"'/' ${DOCKER_DIR}/docker-compose.yml
sed -i 's/@IMAGE_NAME@/'"${IMAGE_NAME}"'/' ${DOCKER_DIR}/docker-compose.yml

if ! [ -x "$(command -v docker-compose)" ]; then
  echo 'ERROR: docker-compose is not installed.' >&2
  echo 'See this document for installation manual:' >&2
  echo '    https://docs.docker.com/compose/install' >&2
  exit 1
fi

echo "${DOCKER_DIR} is ready: $(pwd)/${DOCKER_DIR}"
for i in $(seq ${META_COUNT}); do
    echo "META${i}: ${NODE_IP_PREFIX}.1$((i)):34601"
done
for i in $(seq ${REPLICA_COUNT}); do
    echo "REPLICA${i}: ${NODE_IP_PREFIX}.2$((i)):34801"
done

#!/bin/bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
ROOT=$(
	cd "$(dirname "${SCRIPT_DIR}")" || exit 1
	pwd
)
LOCAL_IP=$(scripts/get_local_ip)

META_COUNT=2
PARTITION_COUNT=8
APP_NAME=temp
META_IP_PREFIX=172.21.0
CLUSTER_NAME=onebox2
IMAGE_NAME=pegasus:latest

DOCKER_DIR=${CLUSTER_NAME}-docker
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
	meta_ip=${META_IP_PREFIX}.1$((i))
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

cp -f "${ROOT}"/docker-compose.yml ${DOCKER_DIR}
sed -i 's/@META_IP_PREFIX@/'"${META_IP_PREFIX}"'/' ${DOCKER_DIR}/docker-compose.yml
sed -i 's/@IMAGE_NAME@/'"${IMAGE_NAME}"'/' ${DOCKER_DIR}/docker-compose.yml

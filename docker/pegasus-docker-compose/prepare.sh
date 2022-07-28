#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
    meta_fqdn=${META_HOSTNAME_PREFIX}.1$((i))
    if [ "${i}" -eq 1 ]; then
        meta_list="${meta_fqdn}:$META_PORT"
    else
        meta_list="$meta_list,${meta_fqdn}:$META_PORT"
    fi
done
sed -i "s/%{meta.server.list}/$meta_list/g" "${DOCKER_DIR}/config.ini"
sed -i "s/%{zk.server.list}/${zookeeper_addr}/g" "${DOCKER_DIR}/config.ini"
sed -i "s/%{meta.port}/$META_PORT/g" "${DOCKER_DIR}/config.ini"

cp -f "${ROOT}"/docker-compose.yml "${DOCKER_DIR}"
for i in $(seq "${META_COUNT}"); do
    meta_port=$((META_PORT+i-1))
    echo "  meta$((i)):
    image: @IMAGE_NAME@
    ports:
      - $meta_port:$meta_port
    volumes:
      - ./config.ini:/pegasus/bin/config.ini:ro
      - ./meta$((i))/data:/pegasus/data
    command:
      - meta
    privileged: true
    hostname: @META_HOSTNAME_PREFIX@.1$((i))
    networks:
      frontend:
        ipv4_address: @META_IP_PREFIX@.1$((i))
    restart: on-failure" >> "${DOCKER_DIR}"/docker-compose.yml
    meta_hostname=$(hostname -f | cut -d' ' -f1)
    echo "META$((i))_ADDRESS=$meta_hostname:$meta_port"
done
for i in $(seq "${REPLICA_COUNT}"); do
    echo "  replica$((i)):
    image: @IMAGE_NAME@
    ports:
      - 34801
    volumes:
      - ./config.ini:/pegasus/bin/config.ini:ro
      - ./replica$((i))/data:/pegasus/data
      - ./replica$((i))/slog:/pegasus/slog
    command:
      - replica
    privileged: true
    restart: on-failure
    networks:
      frontend:" >> "${DOCKER_DIR}"/docker-compose.yml
done
sed -i "s/@META_IP_PREFIX@/${META_IP_PREFIX}/g" "${DOCKER_DIR}"/docker-compose.yml
sed -i "s/@META_HOSTNAME_PREFIX@/${META_HOSTNAME_PREFIX}/g" "${DOCKER_DIR}"/docker-compose.yml
sed -i "s/@IMAGE_NAME@/${IMAGE_NAME}/g" "${DOCKER_DIR}"/docker-compose.yml
sed -i "s/@META_PORT@/${META_PORT}/g" "${DOCKER_DIR}"/docker-compose.yml

echo "${DOCKER_DIR} is ready"

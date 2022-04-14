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

#!/usr/bin/env bash

# Configure the following variables to customize the docker cluster. #

# The ip prefix for each meta.
# Meta-x's ip address is 172.21.0.1{x}:34601.
# For exmaple, Meta1's address is 172.21.0.11:34601.
export META_IP_PREFIX=172.21.0

# The exported port of pegasus meta-server.
# Please ensure this port is not occupied by other programs.
export META_PORT=34601

# Different clusters are isolated by their cluster name and the META_IP_PREFIX.
export CLUSTER_NAME=onebox

export IMAGE_NAME='pegasus:latest'

# allow_non_idempotent_write = true
# for jepsen test this option must be enabled.
export IDEMPOTENT=true
export META_COUNT=1                              # Number of meta instances.
export REPLICA_COUNT=1                           # Number of replica instances.

# Config End #
##############
# The following are constants.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

export ROOT
# Where docker onebox resides. If the cluster name is 'onebox', all nodes will mount their data
# upon the directory ./onebox-docker.
export DOCKER_DIR=${ROOT}/${CLUSTER_NAME}-docker

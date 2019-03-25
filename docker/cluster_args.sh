#!/usr/bin/env bash

# Configure the following variables to customize the docker cluster. #

export PARTITION_COUNT=8
export APP_NAME=temp

# The ip prefix for each nodes.
# Meta-x's ip address is 172.21.0.1{x}:34601.
# Replica-x's ip address is 172.21.0.2{x}:34801
export NODE_IP_PREFIX=172.21.0

export CLUSTER_NAME=onebox2

export IMAGE_NAME=pegasus:latest

# allow_non_idempotent_write = true
# for jepsen test this option must be enabled.
export IDEMPOTENT=true

# Config End #
##############

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT=$(dirname "${SCRIPT_DIR}")
LOCAL_IP=$("${ROOT}"/scripts/get_local_ip)

export SCRIPT_DIR
export ROOT
export LOCAL_IP

export DOCKER_DIR=${ROOT}/${CLUSTER_NAME}-docker # Where docker onebox resides.
export META_COUNT=2 # Number of meta instances.
export REPLICA_COUNT=5 # Number of replica instances.

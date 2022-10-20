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

set -e

# Options:
#    INSTALL_DIR    <dir>

if [ -z "$INSTALL_DIR" ]
then
    echo "ERROR: no INSTALL_DIR specified"
    exit 1
fi

if [ ! -d "$INSTALL_DIR" ]
then
    # ignore zookeeper INSTALL_DIR not exist when stop zookeeper
    exit 0
fi

cd $INSTALL_DIR

# If the old dir for zk bin exists, just use it
ZOOKEEPER_HOME=`pwd`/apache-zookeeper-3.7.0-bin

if [ -d "${ZOOKEEPER_HOME}" ]; then
    ${ZOOKEEPER_HOME}/bin/zkServer.sh stop
    rm -rf ${ZOOKEEPER_HOME} &> /dev/null
    echo "Deleting old zookeeper ... DELETED"
fi

ZOOKEEPER_HOME=`pwd`/zookeeper-bin

if [ -d "${ZOOKEEPER_HOME}" ]; then
    ${ZOOKEEPER_HOME}/bin/zkServer.sh stop
    rm -rf ${ZOOKEEPER_HOME}/data &> /dev/null
    echo "Clearing zookeeper ... CLEARED"
fi

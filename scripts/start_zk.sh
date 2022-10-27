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

CWD=$(cd $(dirname $0) && pwd)

# Options:
#    INSTALL_DIR    <dir>
#    PORT           <port>

if [ -z "$INSTALL_DIR" ]
then
    echo "ERROR: no INSTALL_DIR specified"
    exit 1
fi

if [ -z "$PORT" ]
then
    echo "ERROR: no PORT specified"
    exit 1
fi

if ! mkdir -p "$INSTALL_DIR";
then
    echo "ERROR: mkdir $INSTALL_DIR failed"
    exit 1
fi

cd "$INSTALL_DIR" || exit

# If the old dir for zk bin exists, just use it
ZOOKEEPER_HOME=`pwd`/apache-zookeeper-3.7.0-bin

if [ ! -d "${ZOOKEEPER_HOME}" ]; then
    ${CWD}/download_zk.sh zookeeper-bin
    ZOOKEEPER_HOME=`pwd`/zookeeper-bin
fi

ZOOKEEPER_PORT=$PORT

cp $ZOOKEEPER_HOME/conf/zoo_sample.cfg $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@dataDir=/tmp/zookeeper@dataDir=$ZOOKEEPER_HOME/data@" $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "s@clientPort=2181@clientPort=$ZOOKEEPER_PORT@" $ZOOKEEPER_HOME/conf/zoo.cfg
echo "admin.enableServer=false" >> $ZOOKEEPER_HOME/conf/zoo.cfg
echo "4lw.commands.whitelist=ruok" >> $ZOOKEEPER_HOME/conf/zoo.cfg

mkdir -p $ZOOKEEPER_HOME/data
$ZOOKEEPER_HOME/bin/zkServer.sh start
sleep 1

zk_check_count=0
while true; do
    sleep 1 # wait until zookeeper bootstrapped
    if echo ruok | nc localhost "$ZOOKEEPER_PORT" | grep -q imok; then
        echo "Zookeeper started at port $ZOOKEEPER_PORT"
        exit 0
    fi
    zk_check_count=$((zk_check_count+1))
    echo "ERROR: starting zookeeper has failed ${zk_check_count} times"
    if [ $zk_check_count -gt 30 ]; then
        echo "ERROR: failed to start zookeeper in 30 seconds"
        exit 1
    fi
done

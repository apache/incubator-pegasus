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


if [ ! -f dsn.replication.simple_kv ]; then
    echo "dsn.replication.simple_kv not exist"
    exit 1
fi

./clear.sh

echo "running dsn.replication.simple_kv for 20 seconds ..."
./dsn.replication.simple_kv config.ini &>out &
PID=$!
sleep 20
kill $PID

if [ -f core ] || ! grep ERR_OK out > /dev/null ; then
    echo "run dsn.replication.simple_kv failed"
    echo "---- ls ----"
    ls -l
    echo "---- head -n 100 out ----"
    head -n 100 out
    if [ -f data/logs/log.1.txt ]; then
        echo "---- tail -n 100 log.1.txt ----"
        tail -n 100 data/logs/log.1.txt
    fi
    if [ -f core ]; then
        echo "---- gdb ./dsn.replication.simple_kv core ----"
        gdb ./dsn.replication.simple_kv core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit 1
fi

echo "run dsn.replication.simple_kv succeed"


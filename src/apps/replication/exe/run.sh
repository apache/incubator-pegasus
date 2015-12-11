#!/bin/bash

if [ ! -f dsn.replication.simple_kv ]; then
    echo "dsn.replication.simple_kv not exist"
    exit -1
fi

./clear.sh

echo "running dsn.replication.simple_kv for 30 seconds ..."
./dsn.replication.simple_kv config.ini &>out &
PID=$!
sleep 30
kill $PID

if [ -f core ] || ! grep ERR_OK out; then
    echo "run dsn.replication.simple_kv failed"
    echo "---- ls ----"
    ls -l
    echo "---- tail -n 100 out ----"
    tail -n 100 out
    if find . -name log.1.txt; then
        echo "---- tail -n 100 log.1.txt ----"
        tail -n 100 `find . -name log.1.txt`
    fi
    if [ -f core ]; then
        echo "---- gdb ./dsn.replication.simple_kv core ----"
        gdb ./dsn.replication.simple_kv core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit -1
fi

echo "run dsn.replication.simple_kv succeed"


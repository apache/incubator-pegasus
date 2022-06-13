#!/bin/sh

./dsn_replica_split_test

if [ $? -ne 0 ]; then
    tail -n 100 data/log/log.1.txt
    if [ -f core ]; then
        gdb ./dsn_replica_split_test core -ex "bt"
    fi
    exit 1
fi

#!/bin/bash

./clear.sh
./dsn.tests config-test.ini
if [ $? -ne 0 ]; then
    if ls core.*; then
        gdb ./dsn.tests core.* -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit -1
fi


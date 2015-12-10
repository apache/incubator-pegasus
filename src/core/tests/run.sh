#!/bin/bash

for test_case in `ls config-test*.ini`; do
    echo "============ run dsn.core.tests $test_case ============"
    ./dsn.core.tests ${test_case} <command.txt
    if [ $? -ne 0 ]; then
        echo "run dsn.core.tests $test_case failed"
        ls -l
        if ls core.*; then
            gdb ./dsn.core.tests core.* -ex "thread apply all bt" -ex "set pagination 0" -batch
        fi
        exit -1
    fi
done

echo "============ done ============"


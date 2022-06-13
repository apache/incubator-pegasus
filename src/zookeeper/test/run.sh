#!/bin/bash

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

./clear.sh

output_xml="${REPORT_DIR}/dsn.zookeeper.tests.xml"
GTEST_OUTPUT="xml:${output_xml}" ./dsn.zookeeper.tests config-test.ini

if [ $? -ne 0 ]; then
    echo "run dsn.zookeeper.tests failed"
    echo "---- ls ----"
    ls -l
    if find . -name log.1.txt; then
        echo "---- tail -n 100 log.1.txt ----"
        tail -n 100 `find . -name log.1.txt`
    fi
    if [ -f core ]; then
        echo "---- gdb ./dsn.zookeeper.tests core ----"
        gdb ./dsn.zookeeper.tests core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit 1
fi

#!/bin/sh

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

./clear.sh
output_xml="${REPORT_DIR}/dsn.meta.test.1.xml"
GTEST_OUTPUT="xml:${output_xml}" GTEST_FILTER="-meta.data_definition:meta.apply_balancer" ./dsn.meta.test
if [ $? -ne 0 ]; then
    tail -n 100 data/log/log.1.txt
    if [ -f core ]; then
        echo "---- gdb ./dsn.meta.test core ----"
        gdb ./dsn.meta.test core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit 1
fi

./clear.sh
output_xml="${REPORT_DIR}/dsn.meta.test.2.xml"
GTEST_OUTPUT="xml:${output_xml}" GTEST_FILTER="meta.data_definition" ./dsn.meta.test config-ddl-test.ini
if [ $? -ne 0 ]; then
    tail -n 100 data/log/log.1.txt
    if [ -f core ]; then
        echo "---- gdb ./dsn.meta.test core ----"
        gdb ./dsn.meta.test core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit 1
fi

./clear.sh
output_xml="${REPORT_DIR}/dsn.meta.test.3.xml"
GTEST_OUTPUT="xml:${output_xml}" GTEST_FILTER="meta.apply_balancer" ./dsn.meta.test
if [ $? -ne 0 ]; then
    tail -n 100 data/log/log.1.txt
    if [ -f core ]; then
        echo "---- gdb ./dsn.meta.test core ----"
        gdb ./dsn.meta.test core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit 1
fi


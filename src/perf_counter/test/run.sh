#!/bin/sh

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

./clear.sh
output_xml="${REPORT_DIR}/dsn_perf_counter_test.xml"
GTEST_OUTPUT="xml:${output_xml}" ./dsn_perf_counter_test

#!/bin/sh

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

./clear.sh
output_xml="${REPORT_DIR}/dsn_utils_tests.xml"
GTEST_OUTPUT="xml:${output_xml}" ./dsn_utils_tests

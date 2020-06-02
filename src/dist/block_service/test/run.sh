#!/bin/sh

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

output_xml="${REPORT_DIR}/dsn_block_service_test.xml"
GTEST_OUTPUT="xml:${output_xml}" ./dsn_block_service_test

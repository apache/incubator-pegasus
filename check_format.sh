#!/bin/bash

find . -name '*.h' \
    -o -name '*.cpp' \
    -o -name '*.proto' \
    -o -name '*.thrift' \
    -o -name '*.annotations' \
    -o -name '*.ini' \
    -o -name '*.sh' \
    -o -name '*.php' \
    -o -name '*.act' \
    | grep -v '^./builder/' \
    | grep -v './check_format.sh' \
    | xargs grep -n '	'

if [ $? -eq 0 ]; then
    echo "check format failed"
    exit -1
else
    echo "check format passed"
    exit 0
fi

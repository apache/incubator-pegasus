#!/bin/bash
# This script is to check format of the project, including:
#   - not use 'TAB' in codes, instead using spaces.

find . -name '*.h' \
    -o -name '*.cpp' \
    -o -name '*.proto' \
    -o -name '*.thrift' \
    -o -name '*.annotations' \
    -o -name '*.ini' \
    -o -name '*.sh' \
    -o -name '*.php' \
    -o -name '*.act' \
    | grep -v '^\./\.' \
    | grep -v '^\./builder' \
    | grep -v '^\./scripts/.*/format.sh' \
    | xargs grep -n '	\|<::'

if [ $? -eq 0 ]; then
    echo "ERROR: check format failed: should not contain tab character or '<::'"
    exit 1
else
    echo "Check format succeed"
    exit 0
fi


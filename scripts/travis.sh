#!/bin/bash

# to project root directory
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
cd "$(dirname "$(pwd)")" || exit 1

# ensure source files are well formatted
./scripts/format_files.sh

# ignore updates of submodules
modified=$(git status -s --ignore-submodules)
if [ "$modified" ]; then
    echo "$modified"
    echo "please format the above files before commit"
    exit 1
fi

./run.sh build -c --skip_thirdparty --disable_gperf && ./run.sh test

if [ $? ]; then
    echo "travis failed with exit code $?"
fi

#!/bin/bash

# to project root directory
script_dir=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$script_dir")
cd "${root}" || exit 1

# ensure source files are well formatted
"${root}"/scripts/format_files.sh

# ignore updates of submodules
modified=$(git status -s --ignore-submodules)
if [ "$modified" ]; then
    echo "$modified"
    echo "please format the above files before commit"
    exit 1
fi

"${root}"/run.sh build -c --skip_thirdparty --disable_gperf && ./run.sh test

if [ $? ]; then
    echo "travis failed with exit code $?"
fi

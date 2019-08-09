#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

# lint all scripts, abort if there's any warning.
function shellcheck_must_pass()
{
    if [[ $(shellcheck "$1") ]]; then
        echo "shellcheck $1 failed"
        shellcheck "$1"
        exit 1
    fi
}
shellcheck_must_pass ./scripts/format-all.sh
shellcheck_must_pass ./scripts/travis.sh

# ensure source files are well formatted
./scripts/format-all.sh
if [[ $(git status -s) ]]; then
    git status -s
    echo "please format the above files before commit"
    exit 1
fi

# start pegasus onebox environment
wget https://github.com/XiaoMi/pegasus/releases/download/v1.11.3/pegasus-1.11.3-b45cb06-linux-x86_64-release.zip
unzip pegasus-1.11.3-b45cb06-linux-x86_64-release.zip
cd pegasus-1.11.3-b45cb06-linux-x86_64-release

./run.sh start_onebox -w
cd ../

if ! mvn clean test
then
    cd pegasus-1.11.3-b45cb06-linux-x86_64-release
    ./run.sh list_onebox
    exit 1
fi

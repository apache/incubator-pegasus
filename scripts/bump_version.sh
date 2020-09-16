#!/bin/bash

set -e

if [ $# -ne 1 ]; then
    echo "USAGE: $0 <version>"
    exit 1
fi

pwd="$(cd "$(dirname "$0")" && pwd)"
shell_dir="$(cd "$pwd"/.. && pwd)"
cd "$shell_dir" || exit 1

VERSION=$1
sed -i "s/^#define PEGASUS_VERSION .*/#define PEGASUS_VERSION \"$VERSION\"/" src/include/pegasus/version.h

if ! git rev-parse HEAD; then
    exit 1
fi
git rev-parse HEAD >"${shell_dir}"/GIT_COMMIT

echo "Files modified successfully, version bumped to $VERSION"

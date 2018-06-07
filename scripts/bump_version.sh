#!/bin/bash

if [ $# -ne 1 ]; then
    echo "USAGE: $0 <version>"
    exit 1
fi

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

VERSION=$1
sed -i "s/^#define PEGASUS_VERSION .*/#define PEGASUS_VERSION \"$VERSION\"/" src/include/pegasus/version.h

echo "Files modified successfully, version bumped to $VERSION"


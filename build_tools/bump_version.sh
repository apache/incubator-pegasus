#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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

echo "Files modified successfully, version bumped to $VERSION"

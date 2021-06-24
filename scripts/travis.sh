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

source "${root}"/config_hdfs.sh
"${root}"/run.sh build -c --skip_thirdparty --disable_gperf && ./run.sh test --on_travis
ret=$?
if [ $ret ]; then
    echo "travis failed with exit code $ret"
fi

exit $ret

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

pwd="$( cd "$( dirname "$0"  )" && pwd )"
root_dir="$( cd $pwd/.. && pwd )"
cd $root_dir

linenoise=./src/shell/linenoise
sds=./src/shell/sds
thirdparty=./thirdparty

if [ $# -eq 0 ]; then
  echo "formating all .h/.cpp files in $root_dir ..."
  find . -type f -not \( -wholename "$linenoise/*" -o -wholename "$sds/*" -o -wholename "$thirdparty/*" \) \
      -regextype posix-egrep -regex ".*\.(cpp|h)" | xargs clang-format-14 -i -style=file
elif [ $1 = "-h" ]; then
  echo "USAGE: ./format-files.sh [<relative_path>] -- format .h/.cpp files in $root_dir/relative_path"
  echo "       ./format-files.sh means format all .h/.cpp files in $root_dir"
else
  echo "formating all .h/.cpp files in $root_dir/$1 ..."
  find ./$1 -type f -not \( -wholename "$linenoise/*" -o -wholename "$sds/*" -o -wholename "$thirdparty/*" \) \
      -regextype posix-egrep -regex ".*\.(cpp|h)" | xargs clang-format-14 -i -style=file
fi


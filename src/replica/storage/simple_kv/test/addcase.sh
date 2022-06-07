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


if [ $# -ne 2 ]; then
    echo "USAGE: $0 <new-case-id> <from-case-id>"
    echo " e.g.: $0 106 100"
    exit 1
fi

id=$1

if [ -f case-${id}.act ]; then
    echo "case ${id} already exists"
    exit 1
fi

old=$2
cp case-${old}.act case-${id}.act
cp case-${old}.ini case-${id}.ini


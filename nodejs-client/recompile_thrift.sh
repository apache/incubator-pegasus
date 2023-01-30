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

thrift=thrift

# TODO(yingchun): is it necessary to restrict the thrift version as 0.10.0?
#if ! $thrift -version | grep '0.10.0'
#then
#    echo "Should use thrift 0.10.0"
#    exit -1
#fi

$thrift -v --gen js:node,with_ns -out src/dsn ../idl/dsn.layer2.thrift
$thrift -v --gen js:node,with_ns -out src/dsn ../idl/rrdb.thrift

# TODO(yingchun): There service helpers need to be exported to be used by src/operator.js,
# I didn't find a better method except modifying them manually as follow.
need_export_meta_structs=(
  meta_query_cfg_args
  meta_query_cfg_result
)

need_export_rrdb_structs=(
  rrdb_put_args
  rrdb_put_result
  rrdb_multi_put_args
  rrdb_multi_put_result
  rrdb_remove_args
  rrdb_remove_result
  rrdb_get_args
  rrdb_get_result
  rrdb_multi_get_args
  rrdb_multi_get_result
)

for n in ${need_export_meta_structs[*]}; do
  sed -i "s/var ${n}/var ${n} = module.exports.${n}/g" src/dsn/meta.js
done

for n in ${need_export_rrdb_structs[*]}; do
  sed -i "s/var ${n}/var ${n} = module.exports.${n}/g" src/dsn/rrdb.js
done

echo "done"

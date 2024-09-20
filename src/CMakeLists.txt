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

project(pegasus C CXX)

set(PEGASUS_PROJECT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

set(DSN_BUILD_RUNTIME TRUE)

dsn_common_setup()

if(UNIX)
    add_compile_options(-fPIC)
endif()

include_directories(${PEGASUS_PROJECT_DIR})
include_directories(${PEGASUS_PROJECT_DIR}/common/serialization_helper)
include_directories(${PEGASUS_PROJECT_DIR}/include)
include_directories(${PEGASUS_PROJECT_DIR}/builder/output/include)

add_subdirectory(aio)
add_subdirectory(base)
add_subdirectory(base/test)
add_subdirectory(block_service)
add_subdirectory(client)
add_subdirectory(client_lib)
add_subdirectory(common)
add_subdirectory(failure_detector)
add_subdirectory(geo)
add_subdirectory(gutil)
add_subdirectory(http)
add_subdirectory(meta)
add_subdirectory(nfs)
add_subdirectory(perf_counter)
add_subdirectory(ranger)
add_subdirectory(redis_protocol)
add_subdirectory(remote_cmd)
add_subdirectory(replica)
add_subdirectory(rpc)
add_subdirectory(runtime)
add_subdirectory(sample)
add_subdirectory(security)
add_subdirectory(server)
add_subdirectory(server/test)
add_subdirectory(shell)
add_subdirectory(task)
add_subdirectory(test_util)
add_subdirectory(test/bench_test)
add_subdirectory(test/function_test)
add_subdirectory(test/kill_test)
add_subdirectory(test/pressure_test)
add_subdirectory(tools)
add_subdirectory(utils)
add_subdirectory(zookeeper)

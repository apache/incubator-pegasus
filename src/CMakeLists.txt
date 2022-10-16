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

dsn_common_setup()

include_directories(${PEGASUS_PROJECT_DIR})
include_directories(${PEGASUS_PROJECT_DIR}/include)
include_directories(${PEGASUS_PROJECT_DIR}/rdsn/src)
include_directories(${PEGASUS_PROJECT_DIR}/rdsn/src/common/serialization_helper)

add_subdirectory(rdsn/src)
add_subdirectory(base)
add_subdirectory(reporter)
add_subdirectory(base/test)
add_subdirectory(client_lib)
add_subdirectory(server)
add_subdirectory(server/test)
add_subdirectory(shell)
add_subdirectory(geo)
add_subdirectory(redis_protocol)
add_subdirectory(test/function_test)
add_subdirectory(test/kill_test)
add_subdirectory(test/pressure_test)
add_subdirectory(test/bench_test)
add_subdirectory(sample)

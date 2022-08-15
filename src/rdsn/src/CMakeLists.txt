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

get_filename_component(RDSN_PROJECT_ROOT ${CMAKE_CURRENT_SOURCE_DIR} DIRECTORY)
message (STATUS "RDSN_PROJECT_ROOT = " ${RDSN_PROJECT_ROOT})

# remove?
set(DSN_BUILD_RUNTIME TRUE)

dsn_common_setup()

include_directories(${RDSN_PROJECT_ROOT}/include)
include_directories(${RDSN_PROJECT_ROOT}/include/dsn/cpp/serialization_helper)
include_directories(${RDSN_PROJECT_ROOT}/src)

if(UNIX)
    add_compile_options(-fPIC)
endif()
add_subdirectory(runtime)
add_subdirectory(aio)
add_subdirectory(zookeeper)
add_subdirectory(perf_counter)
add_subdirectory(failure_detector)
add_subdirectory(remote_cmd)
add_subdirectory(nfs)
add_subdirectory(block_service)
add_subdirectory(http)
add_subdirectory(client)
add_subdirectory(common)
add_subdirectory(replica)
add_subdirectory(meta)
add_subdirectory(tools)
add_subdirectory(utils)

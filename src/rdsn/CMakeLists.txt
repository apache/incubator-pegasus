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

project(dsn C CXX)

set(RDSN_PROJECT_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

# remove?
set(DSN_BUILD_RUNTIME TRUE)

dsn_common_setup()

# Users don't have to configure CMAKE_INSTALL_PREFIX unless they want to customize
# the destination.
set(CMAKE_INSTALL_PREFIX ${DSN_ROOT} CACHE STRING "" FORCE)
message (STATUS "dsn Installation directory: CMAKE_INSTALL_PREFIX = " ${CMAKE_INSTALL_PREFIX})

# install rdsn/include to ${CMAKE_INCLUDE_PREFIX}/include
install(DIRECTORY include/ DESTINATION include)

# NOTE: dsn_types.h is not actually a thrift-generated file. As pegasus may reference it,
# we should copy also this file to the path the same as other thrift-generated files.
install(FILES include/dsn/cpp/serialization_helper/dsn_types.h DESTINATION include)
# TODO(wutao1): remove this line after dsn.layer2_types.h is removed from repo.
install(FILES include/dsn/cpp/serialization_helper/dsn.layer2_types.h DESTINATION include)

include_directories(${RDSN_PROJECT_ROOT}/include)
include_directories(${RDSN_PROJECT_ROOT}/include/dsn/cpp/serialization_helper)
include_directories(${RDSN_PROJECT_ROOT}/src)

add_subdirectory(src)

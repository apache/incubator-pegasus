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

cmake_minimum_required(VERSION 3.5.2)

project(pegasus C CXX)

# set PEGASUS_PROJECT_DIR to pegasus/
set(PEGASUS_PROJECT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
get_filename_component(PEGASUS_PROJECT_DIR ${PEGASUS_PROJECT_DIR} DIRECTORY)

# set PEGASUS_ROOT to pegasus/DSN_ROOT, this is where targets will be installed
set(PEGASUS_ROOT ${PEGASUS_PROJECT_DIR}/DSN_ROOT)


include("${PEGASUS_PROJECT_DIR}/rdsn/bin/dsn.cmake")

if(NOT EXISTS "${DSN_ROOT}")
    message(FATAL_ERROR "Please make sure that ${DSN_ROOT} is defined and does exists.")
endif()

if((DSN_THIRDPARTY_ROOT STREQUAL "") OR (NOT EXISTS "${DSN_THIRDPARTY_ROOT}/"))
    message(FATAL_ERROR "Please make sure that ${DSN_THIRDPARTY_ROOT} is defined and does exists.")
endif()

add_definitions(-DGTEST_HAS_TR1_TUPLE=0 -DGTEST_USE_OWN_TR1_TUPLE=0)
dsn_common_setup()

# copy the installed files of rdsn (from rdsn/DSN_ROOT) to pegasus.
execute_process(COMMAND rm -rf ${PEGASUS_ROOT})
execute_process(COMMAND cp -rf ${DSN_ROOT} ${PEGASUS_PROJECT_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${PEGASUS_ROOT}/include)

link_directories(${PEGASUS_ROOT}/lib)

# Users don't have to configure CMAKE_INSTALL_PREFIX unless they want to customize
# the destination.
set(CMAKE_INSTALL_PREFIX ${PEGASUS_ROOT} CACHE STRING "" FORCE)
message (STATUS "pegasus Installation directory: CMAKE_INSTALL_PREFIX = " ${CMAKE_INSTALL_PREFIX})

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
add_subdirectory(test/upgrade_test)
add_subdirectory(test/pressure_test)
add_subdirectory(test/bench_test)

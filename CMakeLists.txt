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

cmake_minimum_required(VERSION 3.24.0)
project(pegasus)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # require at least gcc 7.0.0
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0.0)
        message(FATAL_ERROR "GCC version must be at least 7.0.0!")
    endif ()
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
include(CompilerInfo)
include(BaseFunctions)

# Always generate the compilation database file (compile_commands.json) for use
# with various development tools, such as IWYU and Vim's YouCompleteMe plugin.
# See http://clang.llvm.org/docs/JSONCompilationDatabase.html
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

set(PROJECT_ROOT ${CMAKE_CURRENT_LIST_DIR})
if ("$ENV{THIRDPARTY_ROOT}" STREQUAL "")
    set(THIRDPARTY_ROOT ${PROJECT_ROOT}/thirdparty)
else()
    set(THIRDPARTY_ROOT $ENV{THIRDPARTY_ROOT})
endif()
set(THIRDPARTY_INSTALL_DIR ${THIRDPARTY_ROOT}/output)
message(STATUS "THIRDPARTY_INSTALL_DIR = ${THIRDPARTY_INSTALL_DIR}")

set(BUILD_DIR ${PROJECT_ROOT}/src/builder)
message(STATUS "BUILD_DIR = ${BUILD_DIR}")

option(BUILD_TEST "build unit test" ON)
message(STATUS "BUILD_TEST = ${BUILD_TEST}")

option(ENABLE_GCOV "Enable gcov (for code coverage analysis)" OFF)
message(STATUS "ENABLE_GCOV = ${ENABLE_GCOV}")

# Disable this option before running valgrind.
option(ENABLE_GPERF "Enable gperftools (for tcmalloc)" ON)
message(STATUS "ENABLE_GPERF = ${ENABLE_GPERF}")

option(USE_JEMALLOC "Use jemalloc" OFF)
message(STATUS "USE_JEMALLOC = ${USE_JEMALLOC}")

if(ENABLE_GPERF AND USE_JEMALLOC)
    message(FATAL_ERROR "cannot enable both gperftools and jemalloc simultaneously")
endif()

if(USE_JEMALLOC)
    message(STATUS "jemalloc is enabled")
    set(JEMALLOC_LIB_TYPE "SHARED")
endif()

option(SEPARATE_SERVERS "Whether to build pegasus_collector，pegasus_meta_server and pegasus_replica_server binaries separately, otherwise a combined pegasus_server binary will be built" OFF)
message(STATUS "SEPARATE_SERVERS = ${SEPARATE_SERVERS}")

add_subdirectory(src)

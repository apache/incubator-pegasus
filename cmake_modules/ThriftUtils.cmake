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

find_program(THRIFT_COMPILER
    NAME
        thrift
    PATHS
        ${THIRDPARTY_INSTALL_DIR}/bin
    NO_DEFAULT_PATH
)

set(THRIFT_GENERATED_FILE_PATH ${CMAKE_BINARY_DIR}/thrift-gen CACHE INTERNAL "Where the thrift generated sources locate")
if(NOT EXISTS ${THRIFT_GENERATED_FILE_PATH})
    file(MAKE_DIRECTORY ${THRIFT_GENERATED_FILE_PATH})
endif()
message(STATUS "THRIFT_GENERATED_FILE_PATH=${THRIFT_GENERATED_FILE_PATH}")
include_directories(${THRIFT_GENERATED_FILE_PATH})

# THRIFT_GENERATE_CPP is used to generate sources using the thrift compiler.
#
# Example:
#
# thrift_generate_cpp(
#     REQUEST_META_THRIFT_SRCS
#     REQUEST_META_THRIFT_HDRS
#     ${CMAKE_CURRENT_SOURCE_DIR}/request_meta.thrift
# )
# add_library(
#     dsn_rpc
#     ${REQUEST_META_THRIFT_SRCS}
#     ...
# )
function(THRIFT_GENERATE_CPP SRCS HDRS thrift_file)
    if(NOT EXISTS ${thrift_file})
        message(FATAL_ERROR "thrift file ${thrift_file} does not exist")
    endif()

    message(STATUS "THRIFT_GENERATE_CPP: ${thrift_file}")

    exec_program(${THRIFT_COMPILER}
        ARGS -gen cpp:moveable_types --out ${THRIFT_GENERATED_FILE_PATH} --gen cpp ${thrift_file}
        OUTPUT_VARIABLE __thrift_OUT
        RETURN_VALUE THRIFT_RETURN)
    if(NOT ${THRIFT_RETURN} EQUAL "0")
        message(STATUS "COMMAND: ${THRIFT_COMPILER} -gen cpp:moveable_types --out ${THRIFT_GENERATED_FILE_PATH} --gen cpp ${thrift_file}")
        message(FATAL_ERROR "thrift-compiler exits with " ${THRIFT_RETURN} ": " ${__thrift_OUT})
    endif()

    get_filename_component(__thrift_name ${thrift_file} NAME_WE)

    set(${SRCS})
    set(${HDRS})
    file(GLOB __result_src "${THRIFT_GENERATED_FILE_PATH}/${__thrift_name}_types.cpp")
    file(GLOB __result_hdr "${THRIFT_GENERATED_FILE_PATH}/${__thrift_name}_types.h")
    list(APPEND ${SRCS} ${__result_src})
    list(APPEND ${HDRS} ${__result_hdr})
    # Sets the variables in global scope.
    set(${SRCS} ${${SRCS}} PARENT_SCOPE)
    set(${HDRS} ${${HDRS}} PARENT_SCOPE)

    # install the thrift generated headers to include/
    install(FILES ${__result_hdr} DESTINATION include)
endfunction()

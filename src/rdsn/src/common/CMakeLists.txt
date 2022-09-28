# The MIT License (MIT)
#
# Copyright (c) 2015 Microsoft Corporation
#
# -=- Robust Distributed System Nucleus (rDSN) -=-
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

set(MY_PROJ_NAME dsn_replication_common)

thrift_generate_cpp(
    METADATA_THRIFT_SRCS
    METADATA_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata.thrift
)

thrift_generate_cpp(
    CONSENSUS_THRIFT_SRCS
    CONSENSUS_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/consensus.thrift
)

thrift_generate_cpp(
    DUPLICATION_THRIFT_SRCS
    DUPLICATION_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/duplication.thrift
)

thrift_generate_cpp(
    BACKUP_THRIFT_SRCS
    BACKUP_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/backup.thrift
)

thrift_generate_cpp(
    META_ADMIN_THRIFT_SRCS
    META_ADMIN_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/meta_admin.thrift
)

thrift_generate_cpp(
    BULK_LOAD_THRIFT_SRCS
    BULK_LOAD_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/bulk_load.thrift
)

thrift_generate_cpp(
    PARTITION_SPLIT_THRIFT_SRCS
    PARTITION_SPLIT_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/partition_split.thrift
)

thrift_generate_cpp(
    REPLICA_ADMIN_THRIFT_SRCS
    REPLICA_ADMIN_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/replica_admin.thrift
)

set(MY_PROJ_SRC
    ${DUPLICATION_THRIFT_SRCS}
    ${BACKUP_THRIFT_SRCS}
    ${META_ADMIN_THRIFT_SRCS}
    ${BULK_LOAD_THRIFT_SRCS}
    ${PARTITION_SPLIT_THRIFT_SRCS}
    ${REPLICA_ADMIN_THRIFT_SRCS}
    ${METADATA_THRIFT_SRCS}
    ${CONSENSUS_THRIFT_SRCS}
)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_static_library()

add_subdirectory(test)

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

set(MY_PROJ_NAME pegasus_shell)
project(${MY_PROJ_NAME} C CXX)
set(MY_PROJ_SRC
        linenoise/linenoise.c
        sds/sds.c)
set(MY_SRC_SEARCH_MODE "GLOB_RECURSE")
set(MY_PROJ_LIBS
        pegasus_base
        dsn.replication.tool
        dsn_replica_server
        dsn_meta_server
        dsn_ranger
        dsn_replication_common
        dsn_client
        dsn_http
        dsn_utils
        dsn.block_service.local
        dsn.block_service.hdfs
        dsn.block_service
        dsn.failure_detector
        pegasus_client_static
        pegasus_geo_lib
        rocksdb
        lz4
        zstd
        snappy
        absl::flat_hash_set
        absl::strings
        s2
        hdfs
        curl)
set(MY_BINPLACES
        ${CMAKE_CURRENT_SOURCE_DIR}/config.ini)

set(MY_BOOST_LIBS Boost::system Boost::filesystem)
SET(CMAKE_INSTALL_RPATH ".")
SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
add_definitions(-Wno-attributes)
dsn_add_executable()
dsn_install_executable()

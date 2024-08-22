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

set(SERVER_COMMON_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/server_utils.cpp)
set(COLLECTOR_SRC
        ${SERVER_COMMON_SRC}
        ${CMAKE_CURRENT_SOURCE_DIR}/available_detector.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/info_collector.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/info_collector_app.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/hotspot_partition_calculator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/result_writer.cpp)
set(META_SERVER_SRC
        ${SERVER_COMMON_SRC})
set(REPLICA_SERVER_SRC
        ${SERVER_COMMON_SRC}
        ${CMAKE_CURRENT_SOURCE_DIR}/capacity_unit_calculator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/compaction_filter_rule.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/compaction_operation.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/hotkey_collector.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pegasus_event_listener.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pegasus_manual_compact_service.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pegasus_mutation_duplicator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pegasus_server_impl.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pegasus_server_impl_init.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pegasus_server_write.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/pegasus_write_service.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb_wrapper.cpp)

set(SERVER_COMMON_LIBS
        dsn_utils)
set(COLLECTOR_LIBS
        ${SERVER_COMMON_LIBS}
        dsn_meta_server
        pegasus_client_static
        event)
set(META_SERVER_LIBS
        ${SERVER_COMMON_LIBS}
        dsn_meta_server
        dsn.failure_detector
        dsn.replication.zookeeper_provider
        dsn.block_service
        event
        zookeeper
        hashtable)
set(REPLICA_SERVER_LIBS
        ${SERVER_COMMON_LIBS}
        dsn_replica_server
        dsn_replication_common
        dsn.block_service.local
        dsn.block_service
        dsn.failure_detector
        rocksdb
        pegasus_base
        pegasus_client_static
        event)

if (SEPARATE_SERVERS)
    add_subdirectory(collector)
    add_subdirectory(meta_server)
    add_subdirectory(replica_server)
else ()
    set(MY_PROJ_NAME pegasus_server)
    set(MY_PROJ_SRC "")
    set(MY_SRC_SEARCH_MODE "GLOB")
    set(MY_PROJ_LIBS
            ${SERVER_COMMON_LIBS}
            ${COLLECTOR_LIBS}
            ${META_SERVER_LIBS}
            ${REPLICA_SERVER_LIBS})
    set(MY_BOOST_LIBS Boost::system Boost::filesystem)
    set(MY_BINPLACES config.ini)
    SET(CMAKE_INSTALL_RPATH ".")
    SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
    dsn_add_executable()
    dsn_install_executable()
endif ()

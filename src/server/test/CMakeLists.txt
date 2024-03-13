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

set(MY_PROJ_NAME pegasus_unit_test)
set(MY_PROJ_SRC
        "../pegasus_server_impl.cpp"
        "../pegasus_server_impl_init.cpp"
        "../pegasus_manual_compact_service.cpp"
        "../pegasus_event_listener.cpp"
        "../pegasus_write_service.cpp"
        "../pegasus_server_write.cpp"
        "../capacity_unit_calculator.cpp"
        "../pegasus_mutation_duplicator.cpp"
        "../hotspot_partition_calculator.cpp"
        "../hotkey_collector.cpp"
        "../rocksdb_wrapper.cpp"
        "../compaction_filter_rule.cpp"
        "../compaction_operation.cpp")

set(MY_SRC_SEARCH_MODE "GLOB")
set(MY_PROJ_LIBS
        dsn_replica_server
        dsn_meta_server
        dsn_ranger
        dsn_replication_common
        dsn_client
        dsn.block_service.local
        dsn.block_service
        dsn.failure_detector
        dsn.replication.zookeeper_provider
        dsn_utils
        rocksdb
        lz4
        zstd
        snappy
        pegasus_client_static
        event
        pegasus_base
        gtest
        gmock
        hashtable)
add_definitions(-DPEGASUS_UNIT_TEST)
add_definitions(-DENABLE_FAIL)
set(MY_BOOST_LIBS Boost::system Boost::filesystem)
set(MY_BINPLACES
        config.ini
        run.sh)
dsn_add_test()

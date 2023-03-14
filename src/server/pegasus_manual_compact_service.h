/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <stdint.h>
#include <atomic>
#include <map>
#include <string>

#include "metadata_types.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica/replica_base.h"

namespace rocksdb {
struct CompactRangeOptions;
} // namespace rocksdb

namespace pegasus {
namespace server {

class pegasus_server_impl;

class pegasus_manual_compact_service : public dsn::replication::replica_base
{
public:
    explicit pegasus_manual_compact_service(pegasus_server_impl *app);

    void init_last_finish_time_ms(uint64_t last_finish_time_ms);

    void start_manual_compact_if_needed(const std::map<std::string, std::string> &envs);

    // Called by pegasus_manual_compaction.sh
    std::string query_compact_state() const;

    dsn::replication::manual_compaction_status::type query_compact_status() const;

private:
    friend class manual_compact_service_test;

    // return true if manual compaction is disabled.
    bool check_compact_disabled(const std::map<std::string, std::string> &envs);

    // return max concurrent count.
    int check_compact_max_concurrent_running_count(const std::map<std::string, std::string> &envs);

    // return true if need do once manual compaction.
    bool check_once_compact(const std::map<std::string, std::string> &envs);

    // return true if need do periodic manual compaction.
    bool check_periodic_compact(const std::map<std::string, std::string> &envs);

    void extract_manual_compact_opts(const std::map<std::string, std::string> &envs,
                                     const std::string &key_prefix,
                                     rocksdb::CompactRangeOptions &options);

    void manual_compact(const rocksdb::CompactRangeOptions &options);

    // return manual compact start time in ms.
    uint64_t begin_manual_compact();

    void end_manual_compact(uint64_t start, uint64_t finish);

    // return true when allow to start a new manual compact,
    // otherwise return false
    bool check_manual_compact_state();

    uint64_t now_timestamp();

private:
    pegasus_server_impl *_app;
#ifdef PEGASUS_UNIT_TEST
    uint64_t _mock_now_timestamp = 0;
#endif

    // manual compact state
    std::atomic<bool> _disabled;
    std::atomic<int> _max_concurrent_running_count;
    std::atomic<uint64_t> _manual_compact_enqueue_time_ms;
    std::atomic<uint64_t> _manual_compact_start_running_time_ms;
    std::atomic<uint64_t> _manual_compact_last_finish_time_ms;
    std::atomic<uint64_t> _manual_compact_last_time_used_ms;

    ::dsn::perf_counter_wrapper _pfc_manual_compact_enqueue_count;
    ::dsn::perf_counter_wrapper _pfc_manual_compact_running_count;
};

} // namespace server
} // namespace pegasus

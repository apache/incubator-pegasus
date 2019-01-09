// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rocksdb/db.h>
#include <dsn/utility/string_view.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <dsn/dist/replication/replica_base.h>

namespace pegasus {
namespace server {

class pegasus_server_impl;

class pegasus_manual_compact_service : public dsn::replication::replica_base
{
public:
    explicit pegasus_manual_compact_service(pegasus_server_impl *app);

    void init_last_finish_time_ms(uint64_t last_finish_time_ms);

    void start_manual_compact_if_needed(const std::map<std::string, std::string> &envs);

    std::string query_compact_state() const;

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
    int32_t _manual_compact_min_interval_seconds;

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

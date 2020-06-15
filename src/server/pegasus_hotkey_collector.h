// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <functional>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>
#include <gtest/gtest_prod.h>
#include <readerwriterqueue/readerwriterqueue.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>
#include <dsn/dist/replication/replica_base.h>
#include "base/pegasus_utils.h"

namespace pegasus {
namespace server {

DSN_DECLARE_int32(data_capture_hash_bucket_num);

typedef std::shared_ptr<std::unordered_map<int, int>> thread_queue_map;

class hotkey_coarse_data_collector;
class hotkey_fine_data_collector;

enum class collector_state
{
    STOP,   // data has been cleared, ready to start
    COARSE, // is running corase capture and analyse
    FINE,   // is running fine capture and analyse
    FINISH  // capture and analyse is done, ready to get result
};

class hotkey_collector : public dsn::replication::replica_base
{
public:
    hotkey_collector(dsn::apps::hotkey_type::type hotkey_type,
                     dsn::replication::replica_base *r_base);
    // after receiving START RPC, start to capture and analyse
    bool init();
    // after receiving collector_state::STOP RPC or timeout, clear historical data
    void clear();
    void capture_blob_data(const ::dsn::blob &key, int count = 1);
    void capture_msg_data(dsn::message_ex **requests_point, const int count);
    void capture_str_data(const std::string &data, int count);
    void capture_multi_get_data(const ::dsn::apps::multi_get_request &request,
                                const ::dsn::apps::multi_get_response &resp);
    // analyse_data is a periodic task, only valid when _collector_state == collector_state::COARSE
    // || collector_state::FINE
    void analyse_data();
    std::string get_status();
    // ture: result = hotkey, false: can't find hotkey
    bool get_result(std::string &result);
    // Like hotspot_algo_qps_variance, we use PauTa Criterion to find the hotkey
    static bool variance_calc(const std::vector<int> &data_samples,
                              std::vector<int> &hot_values,
                              const int threshold);
    static int data_hash_method(const std::string &data);
    int get_coarse_result() const { return _coarse_result; }

    // hotkey_type == READ, using THREAD_POOL_LOCAL_APP threadpool to distribute queue
    // hotkey_type == WRITE, using single queue
    dsn::apps::hotkey_type::type hotkey_type;

private:
    std::atomic<collector_state> _collector_state;
    // By _collector_start_time && FLAGS_hotkey_collector_max_work_time it can stop automatically
    uint64_t _collector_start_time;
    std::unique_ptr<hotkey_coarse_data_collector> _coarse_data_collector;
    std::unique_ptr<hotkey_fine_data_collector> _fine_data_collector;
    std::string _fine_result;
    int _coarse_result;

    FRIEND_TEST(hotkey_collector_test, init_destory_timeout);
};

class hotkey_coarse_data_collector : public dsn::replication::replica_base
{
public:
    hotkey_coarse_data_collector(hotkey_collector *base);
    void capture_coarse_data(const std::string &data, int count);
    const int analyse_coarse_data();

private:
    std::vector<std::atomic<int>> _coarse_hash_buckets;
};

class hotkey_fine_data_collector : public dsn::replication::replica_base
{
public:
    hotkey_fine_data_collector(hotkey_collector *base);
    void capture_fine_data(const std::string &data, int count);
    bool analyse_fine_data(std::string &result);

private:
    int _target_bucket;
    thread_queue_map _thread_queue_map;
    std::vector<moodycamel::ReaderWriterQueue<std::pair<std::string, int>>> _string_capture_queue;
    uint64_t _fine_data_variance_threshold;

    inline int get_queue_index();
};

} // namespace server
} // namespace pegasus

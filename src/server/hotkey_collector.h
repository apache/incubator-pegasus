// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <dsn/dist/replication/replication_types.h>
#include <concurrentqueue/concurrentqueue.h>
#include <dsn/dist/replication/replica_base.h>
#include <dsn/utility/flags.h>
#include <gtest/gtest_prod.h>

#include "hotkey_collector_state.h"

namespace pegasus {
namespace server {

class internal_collector_base;

struct detect_hotkey_result
{
    int coarse_bucket_index = -1;
    std::string hot_hash_key = "";
};

DSN_DECLARE_uint32(hotkey_buckets_num);
int get_bucket_id(dsn::string_view data);
bool find_outlier_index(const std::vector<uint64_t> &captured_keys, int threshold, int &hot_index);

//    hotkey_collector is responsible to find the hot keys after the partition
//    was detected to be hot. The two types of hotkey, READ & WRITE, are detected
//    separately.
//
//    +--------------------+  +----------------------------------------------------+
//    |   Replcia server   |  | Hotkey collector                                   |
//    |                    |  | +-----------------------------------------------+  |
//    | +----------------+ |  | | Corase capture                                |  |
//    | |                | |--> |                 +----------+                  |  |
//    | |  RPC received  | || | |                 |   Data   |                  |  |
//    | |                | || | |                 +-----+----+                  |  |
//    | +-------+--------+ || | |                       |                       |  |
//    |         |          || | |  +---------------+----v--+-------+---------+  |  |
//    |         v          || | |  |       |Hot    |       |       |         |  |  |
//    | +-------+--------+ || | |  |Bucket |Bucket |Bucket |Bucket |Bucket   |  |  |
//    | |   Replication  | || | |  +-----------+-----------------------------+  |  |
//    | | (only on the   | || | |              |                                |  |
//    | |  write path))  | || | +--------------|--------------------------------+  |
//    | +-------+--------+ || |             +--v---+                               |
//    |         |          || |             | Data |                               |
//    |         v          || |             +------+                               |
//    | +-------+--------+ || |          +-----|-------+-------------+             |
//    | |                | || |   +------|-------------|-------------|---------+   |
//    | |  Capture data  ---| |   | Fine |capture      |             |         |   |
//    | |                | |  |   |      |             |             |         |   |
//    | +-------+--------+ |  |   | +----v----+   +----v----+   +----v----+    |   |
//    |         |          |  |   | |  queue  |   |  queue  |   |  queue  |    |   |
//    |         v          |  |   | +----+----+   +----+----+   +----+----+    |   |
//    | +-------+--------+ |  |   |      |             |             |         |   |
//    | |                | |  |   | +----v-------------v-------------v------+  |   |
//    | |   Place data   | |  |   | |             Analsis pool              |  |   |
//    | |   to the disk  | |  |   | +-----------------|---------------------+  |   |
//    | |                | |  |   +-------------------|------------------------+   |
//    | +----------------+ |  |                       v                            |
//    |                    |  |                     Hotkey                         |
//    +--------------------+  +----------------------------------------------------+

class hotkey_collector : public dsn::replication::replica_base
{
public:
    hotkey_collector(dsn::replication::hotkey_type::type hotkey_type,
                     dsn::replication::replica_base *r_base);
    // TODO: (Tangyanzhao) capture_*_key should be consistent with hotspot detection
    // weight: calculate the weight according to the specific situation
    void capture_raw_key(const dsn::blob &raw_key, int64_t weight);
    void capture_hash_key(const dsn::blob &hash_key, int64_t weight);
    void analyse_data();
    void handle_rpc(const dsn::replication::detect_hotkey_request &req,
                    /*out*/ dsn::replication::detect_hotkey_response &resp);

private:
    void on_start_detect(dsn::replication::detect_hotkey_response &resp);
    void on_stop_detect(dsn::replication::detect_hotkey_response &resp);
    void terminate();
    bool terminate_if_timeout();

    static int get_bucket_id(dsn::string_view data);
    static bool
    find_outlier_index(const std::vector<uint64_t> &captured_keys, int threshold, int &hot_index);

    detect_hotkey_result _result;
    std::atomic<hotkey_collector_state> _state;
    const dsn::replication::hotkey_type::type _hotkey_type;
    std::shared_ptr<internal_collector_base> _internal_collector;
    uint64_t _collector_start_time_second;
};

// Be sure every function in internal_collector_base should be thread safe
class internal_collector_base : public dsn::replication::replica_base
{
public:
    explicit internal_collector_base(replica_base *base) : replica_base(base){};
    virtual void capture_data(const dsn::blob &hash_key, uint64_t weight) = 0;
    virtual void analyse_data(detect_hotkey_result &result) = 0;
    virtual void clear() = 0;
};

// used in hotkey_collector_state::STOPPED and hotkey_collector_state::FINISHED, avoid null pointers
class hotkey_empty_data_collector : public internal_collector_base
{
public:
    explicit hotkey_empty_data_collector(replica_base *base) : internal_collector_base(base) {}
    void capture_data(const dsn::blob &hash_key, uint64_t weight) override {}
    void analyse_data(detect_hotkey_result &result) override {}
    void clear() override {}
};

// TODO: (Tangyanzhao) add a unit test of hotkey_coarse_data_collector
class hotkey_coarse_data_collector : public internal_collector_base
{
public:
    explicit hotkey_coarse_data_collector(replica_base *base);
    void capture_data(const dsn::blob &hash_key, uint64_t weight) override;
    void analyse_data(detect_hotkey_result &result) override;
    void clear() override;

private:
    hotkey_coarse_data_collector() = delete;

    std::vector<std::atomic<uint64_t>> _hash_buckets;

    friend class coarse_collector_test;
};

class hotkey_fine_data_collector : public internal_collector_base
{
public:
    hotkey_fine_data_collector(replica_base *base, int target_bucket_index, int max_queue_size);
    void capture_data(const dsn::blob &hash_key, uint64_t weight) override;
    void analyse_data(detect_hotkey_result &result) override;
    void clear() override;

private:
    hotkey_fine_data_collector() = delete;

    const uint32_t _max_queue_size;
    const uint32_t _target_bucket_index;
    // ConcurrentQueue is a lock-free queue to capture keys
    moodycamel::ConcurrentQueue<std::pair<dsn::blob, uint64_t>> _capture_key_queue;

    friend class fine_collector_test;
};

} // namespace server
} // namespace pegasus

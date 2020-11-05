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

#include "hotkey_collector.h"

#include <dsn/dist/replication/replication_enums.h>
#include <dsn/utility/smart_pointers.h>
#include <dsn/utility/flags.h>
#include <boost/functional/hash.hpp>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>
#include <dsn/tool-api/task.h>
#include <dsn/dist/replication/replication.codes.h>
#include "base/pegasus_key_schema.h"

namespace pegasus {
namespace server {

DSN_DEFINE_uint32(
    "pegasus.server",
    hot_bucket_variance_threshold,
    3,
    "the variance threshold to detect hot bucket during coarse analysis of hotkey detection");

DSN_DEFINE_uint32(
    "pegasus.server",
    hot_key_variance_threshold,
    3,
    "the variance threshold to detect hot key during fine analysis of hotkey detection");

// TODO: (Tangyanzhao) add a limit to avoid changing when detecting
DSN_DEFINE_uint32("pegasus.server",
                  hotkey_buckets_num,
                  37,
                  "the number of data capture hash buckets");

DSN_DEFINE_validator(hotkey_buckets_num, [](int32_t bucket_num) -> bool {
    if (bucket_num < 3) {
        return false;
    }
    // hotkey_buckets_num should be a prime number
    for (int i = 2; i <= bucket_num / i; i++) {
        if (bucket_num % i == 0) {
            return false;
        }
    }
    return true;
});

DSN_DEFINE_int32(
    "pegasus.server",
    max_seconds_to_detect_hotkey,
    150,
    "the max time (in seconds) allowed to capture hotkey, will stop if hotkey's not found");

// 68–95–99.7 rule, same algorithm as hotspot_partition_calculator::stat_histories_analyse
static bool
find_outlier_index(const std::vector<uint64_t> &captured_keys, int threshold, int &hot_index)
{
    dcheck_gt(captured_keys.size(), 2);
    int data_size = captured_keys.size();
    // empirical rule to calculate hot point of each partition
    // same algorithm as hotspot_partition_calculator::stat_histories_analyse
    double table_captured_key_sum = 0;
    int hot_value = 0;
    for (int i = 0; i < data_size; i++) {
        table_captured_key_sum += captured_keys[i];
        if (captured_keys[i] > hot_value) {
            hot_index = i;
            hot_value = captured_keys[i];
        }
    }
    // TODO: (Tangyanzhao) increase a judgment of table_captured_key_sum
    double captured_keys_avg_count =
        (table_captured_key_sum - captured_keys[hot_index]) / (data_size - 1);
    double standard_deviation = 0;
    for (int i = 0; i < data_size; i++) {
        if (i != hot_index) {
            standard_deviation += pow((captured_keys[i] - captured_keys_avg_count), 2);
        }
    }
    standard_deviation = sqrt(standard_deviation / (data_size - 2));
    double hot_point = (hot_value - captured_keys_avg_count) / standard_deviation;
    if (hot_point >= threshold) {
        return true;
    } else {
        hot_index = -1;
        return false;
    }
}

// TODO: (Tangyanzhao) replace it to xxhash
static int get_bucket_id(dsn::string_view data)
{
    size_t hash_value = boost::hash_range(data.begin(), data.end());
    return static_cast<int>(hash_value % FLAGS_hotkey_buckets_num);
}

hotkey_collector::hotkey_collector(dsn::replication::hotkey_type::type hotkey_type,
                                   dsn::replication::replica_base *r_base)
    : replica_base(r_base),
      _hotkey_type(hotkey_type),
      _internal_collector(std::make_shared<hotkey_empty_data_collector>(this)),
      _collector_start_time_second(0)
{
}

void hotkey_collector::handle_rpc(const dsn::replication::detect_hotkey_request &req,
                                  dsn::replication::detect_hotkey_response &resp)
{
    switch (req.action) {
    case dsn::replication::detect_action::START:
        on_start_detect(resp);
        return;
    case dsn::replication::detect_action::STOP:
        on_stop_detect(resp);
        return;
    default:
        std::string hint = fmt::format("{}: can't find this detect action", req.action);
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        derror_replica(hint);
    }
}

void hotkey_collector::capture_raw_key(const dsn::blob &raw_key, int64_t weight)
{
    dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);
    capture_hash_key(hash_key, weight);
}

void hotkey_collector::capture_hash_key(const dsn::blob &hash_key, int64_t weight)
{
    // TODO: (Tangyanzhao) add a unit test to ensure data integrity
    _internal_collector->capture_data(hash_key, weight);
}

void hotkey_collector::analyse_data()
{
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
        if (!terminate_if_timeout()) {
            _internal_collector->analyse_data(_result);
            if (_result.coarse_bucket_index != -1) {
                // TODO: (Tangyanzhao) reset _internal_collector to hotkey_fine_data_collector
                _state.store(hotkey_collector_state::FINE_DETECTING);
            }
        }
        return;
    default:
        return;
    }
}

void hotkey_collector::on_start_detect(dsn::replication::detect_hotkey_response &resp)
{
    auto now_state = _state.load();
    std::string hint;
    switch (now_state) {
    case hotkey_collector_state::COARSE_DETECTING:
    case hotkey_collector_state::FINE_DETECTING:
        resp.err = dsn::ERR_INVALID_STATE;
        hint = fmt::format("still detecting {} hotkey, state is {}",
                           dsn::enum_to_string(_hotkey_type),
                           enum_to_string(now_state));
        dwarn_replica(hint);
        return;
    case hotkey_collector_state::FINISHED:
        resp.err = dsn::ERR_INVALID_STATE;
        hint = fmt::format(
            "{} hotkey result has been found, you can send a stop rpc to restart hotkey detection",
            dsn::enum_to_string(_hotkey_type));
        dwarn_replica(hint);
        return;
    case hotkey_collector_state::STOPPED:
        _collector_start_time_second = dsn_now_s();
        _internal_collector.reset(new hotkey_coarse_data_collector(this));
        _state.store(hotkey_collector_state::COARSE_DETECTING);
        resp.err = dsn::ERR_OK;
        hint = fmt::format("starting to detect {} hotkey", dsn::enum_to_string(_hotkey_type));
        ddebug_replica(hint);
        return;
    default:
        hint = "invalid collector state";
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        derror_replica(hint);
        dassert(false, "invalid collector state");
    }
}

void hotkey_collector::on_stop_detect(dsn::replication::detect_hotkey_response &resp)
{
    terminate();
    resp.err = dsn::ERR_OK;
    std::string hint =
        fmt::format("{} hotkey stopped, cache cleared", dsn::enum_to_string(_hotkey_type));
    ddebug_replica(hint);
}

void hotkey_collector::terminate()
{
    _state.store(hotkey_collector_state::STOPPED);
    _internal_collector.reset();
    _collector_start_time_second = 0;
}

bool hotkey_collector::terminate_if_timeout()
{
    if (dsn_now_s() >= _collector_start_time_second + FLAGS_max_seconds_to_detect_hotkey) {
        ddebug_replica("hotkey collector work time is exhausted but no hotkey has been found");
        terminate();
        return true;
    }
    return false;
}

hotkey_coarse_data_collector::hotkey_coarse_data_collector(replica_base *base)
    : internal_collector_base(base), _hash_buckets(FLAGS_hotkey_buckets_num)
{
    for (auto &bucket : _hash_buckets) {
        bucket.store(0);
    }
}

void hotkey_coarse_data_collector::capture_data(const dsn::blob &hash_key, uint64_t weight)
{
    _hash_buckets[get_bucket_id(hash_key)].fetch_add(weight);
}

void hotkey_coarse_data_collector::analyse_data(detect_hotkey_result &result)
{
    std::vector<uint64_t> buckets(FLAGS_hotkey_buckets_num);
    for (int i = 0; i < buckets.size(); i++) {
        buckets[i] = _hash_buckets[i].load();
        _hash_buckets[i].store(0);
    }
    if (!find_outlier_index(
            buckets, FLAGS_hot_bucket_variance_threshold, result.coarse_bucket_index)) {
        result.coarse_bucket_index = -1;
    }
}

hotkey_fine_data_collector::hotkey_fine_data_collector(replica_base *base,
                                                       int target_bucket_index,
                                                       int max_queue_size)
    : internal_collector_base(base),
      _max_queue_size(max_queue_size),
      _target_bucket_index(target_bucket_index),
      _capture_key_queue(max_queue_size)
{
}

void hotkey_fine_data_collector::capture_data(const dsn::blob &hash_key, uint64_t weight)
{
    if (get_bucket_id(hash_key) != _target_bucket_index) {
        return;
    }
    // abandon the key if enqueue failed (possibly because not enough room to enqueue)
    _capture_key_queue.try_enqueue(std::make_pair(hash_key, weight));
}

struct blob_hash
{
    std::size_t operator()(const dsn::blob &str) const
    {
        dsn::string_view cp(str);
        return boost::hash_range(cp.begin(), cp.end());
    }
};

struct blob_equal
{
    std::size_t operator()(const dsn::blob &lhs, const dsn::blob &rhs) const
    {
        return dsn::string_view(lhs) == dsn::string_view(rhs);
    }
};

void hotkey_fine_data_collector::analyse_data(detect_hotkey_result &result)
{
    // hashkey -> weight
    std::unordered_map<dsn::blob, uint64_t, blob_hash, blob_equal> hash_keys_weight;
    std::pair<dsn::blob, uint64_t> key_weight_pair;
    // prevent endless loop, limit the number of elements analyzed not to exceed the queue size
    uint32_t dequeue_cnt = 0;
    while (_capture_key_queue.try_dequeue(key_weight_pair) && ++dequeue_cnt <= _max_queue_size) {
        hash_keys_weight[key_weight_pair.first] += key_weight_pair.second;
    }

    if (hash_keys_weight.empty()) {
        return;
    }

    std::vector<uint64_t> hash_key_counts;
    hash_key_counts.reserve(hash_keys_weight.size());
    dsn::string_view count_max_key;
    uint64_t count_max = 0;
    for (const auto &iter : hash_keys_weight) {
        hash_key_counts.push_back(iter.second);
        if (iter.second > count_max) {
            count_max = iter.second;
            // the key with the max accessed count.
            count_max_key = iter.first;
        }
    }

    // hash_key_counts stores the number of occurrences of each string captured in a period of time
    // the size of hash_key_counts influences our hotkey determination strategy
    // hash_key_counts.size() == 1: the only key must be the hotkey
    // hash_key_counts.size() == 2: the hotkey is the larger one
    // hash_key_counts.size() >= 3: use find_outlier_index to determinate whether exist a hotkey
    int hot_index;
    if (hash_key_counts.size() < 3 ||
        find_outlier_index(hash_key_counts, FLAGS_hot_key_variance_threshold, hot_index)) {
        result.hot_hash_key = std::string(count_max_key);
    }
}

} // namespace server
} // namespace pegasus

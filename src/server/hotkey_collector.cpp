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

#include <boost/container_hash/hash.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <string_view>
#include <unordered_map>

#include "base/pegasus_key_schema.h"
#include "base/pegasus_utils.h"
#include "common/replication_enums.h"
#include "runtime/api_layer1.h"
#include "server/hotkey_collector_state.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

DSN_DEFINE_uint32(
    pegasus.server,
    hot_bucket_variance_threshold,
    7,
    "the variance threshold to detect hot bucket during coarse analysis of hotkey detection");
DSN_TAG_VARIABLE(hot_bucket_variance_threshold, FT_MUTABLE);

DSN_DEFINE_uint32(
    pegasus.server,
    hot_key_variance_threshold,
    5,
    "the variance threshold to detect hot key during fine analysis of hotkey detection");
DSN_TAG_VARIABLE(hot_key_variance_threshold, FT_MUTABLE);

DSN_DEFINE_uint32(pegasus.server,
                  hotkey_buckets_num,
                  37,
                  "the number of data capture hash buckets");

DSN_DEFINE_validator(hotkey_buckets_num, [](uint32_t bucket_num) -> bool {
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

DSN_DEFINE_uint32(
    pegasus.server,
    max_seconds_to_detect_hotkey,
    150,
    "the max time (in seconds) allowed to capture hotkey, will stop if hotkey's not found");
DSN_TAG_VARIABLE(max_seconds_to_detect_hotkey, FT_MUTABLE);

namespace pegasus {
namespace server {

// 68–95–99.7 rule, same algorithm as hotspot_partition_calculator::stat_histories_analyse
/*extern*/ bool
find_outlier_index(const std::vector<uint64_t> &captured_keys, int threshold, int &hot_index)
{
    CHECK_GT(captured_keys.size(), 2);
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
    if (hot_index == -1) {
        return false;
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

    // There are two cases will lead standard_deviation = 0
    // Case 1: have hotspot [1, 1, 300, 1]
    // Case 2: not have hotspot [1, 1, 1, 1]
    // In both case 1 and case 2, we select [1, 1, 1] to calculate standard_deviation, so it equals
    // to 0. In these scenes, we simply compare the hot_value with the average
    if (standard_deviation == 0) {
        return hot_value > captured_keys_avg_count;
    }

    double hot_point = (hot_value - captured_keys_avg_count) / standard_deviation;
    if (hot_point >= threshold) {
        return true;
    } else {
        hot_index = -1;
        return false;
    }
}

// TODO: (Tangyanzhao) replace it to xxhash

/*extern*/ int get_bucket_id(std::string_view data, int bucket_num)
{
    return static_cast<int>(boost::hash_range(data.begin(), data.end()) % bucket_num);
}

hotkey_collector::hotkey_collector(dsn::replication::hotkey_type::type hotkey_type,
                                   dsn::replication::replica_base *r_base)
    : replica_base(r_base), _hotkey_type(hotkey_type)
{
    int now_hash_bucket_num = FLAGS_hotkey_buckets_num;
    _internal_coarse_collector =
        std::make_shared<hotkey_coarse_data_collector>(this, now_hash_bucket_num);
    _internal_fine_collector =
        std::make_shared<hotkey_fine_data_collector>(this, now_hash_bucket_num);
    _internal_empty_collector = std::make_shared<hotkey_empty_data_collector>(this);
    _state.store(hotkey_collector_state::STOPPED);
}

inline void hotkey_collector::change_state_to_stopped()
{
    _state.store(hotkey_collector_state::STOPPED);
    _result.if_find_result.store(false);
    _internal_coarse_collector->clear();
    _internal_fine_collector->clear();
}

inline void hotkey_collector::change_state_to_coarse_detecting()
{
    _state.store(hotkey_collector_state::COARSE_DETECTING);
    _collector_start_time_second.store(dsn_now_s());
}

inline void hotkey_collector::change_state_to_fine_detecting()
{
    _state.store(hotkey_collector_state::FINE_DETECTING);
    _internal_fine_collector->change_target_bucket(_result.coarse_bucket_index);
}

inline void hotkey_collector::change_state_to_finished()
{
    _state.store(hotkey_collector_state::FINISHED);
    _result.if_find_result.store(true);
}

inline std::shared_ptr<internal_collector_base> hotkey_collector::get_internal_collector_by_state()
{
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
        return _internal_coarse_collector;
    case hotkey_collector_state::FINE_DETECTING:
        return _internal_fine_collector;
    default:
        return _internal_empty_collector;
    }
}

inline void hotkey_collector::change_state_by_result()
{
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
        if (_result.coarse_bucket_index != -1) {
            change_state_to_fine_detecting();
        }
        break;
    case hotkey_collector_state::FINE_DETECTING:
        if (!_result.hot_hash_key.empty()) {
            change_state_to_finished();
            LOG_ERROR_PREFIX("Find the hotkey: {}",
                             pegasus::utils::c_escape_sensitive_string(_result.hot_hash_key));
        }
        break;
    default:
        break;
    }
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
    case dsn::replication::detect_action::QUERY:
        query_result(resp);
        return;
    default:
        std::string hint = fmt::format("{}: can't find this detect action", req.action);
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        LOG_ERROR_PREFIX(hint);
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
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
    case hotkey_collector_state::FINE_DETECTING:
        get_internal_collector_by_state()->capture_data(hash_key, weight > 0 ? weight : 1);
        return;
    default:
        return;
    }
}

void hotkey_collector::analyse_data()
{
    switch (_state.load()) {
    case hotkey_collector_state::COARSE_DETECTING:
    case hotkey_collector_state::FINE_DETECTING:
        if (!terminate_if_timeout()) {
            get_internal_collector_by_state()->analyse_data(_result);
            change_state_by_result();
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
        resp.err = dsn::ERR_BUSY;
        hint = fmt::format("still detecting {} hotkey, state is {}",
                           dsn::enum_to_string(_hotkey_type),
                           enum_to_string(now_state));
        break;
    case hotkey_collector_state::FINISHED:
        resp.err = dsn::ERR_BUSY;
        hint = fmt::format("{} hotkey result has been found: {}, you can send a stop rpc to "
                           "restart hotkey detection",
                           dsn::enum_to_string(_hotkey_type),
                           pegasus::utils::c_escape_sensitive_string(_result.hot_hash_key));
        break;
    case hotkey_collector_state::STOPPED:
        change_state_to_coarse_detecting();
        resp.err = dsn::ERR_OK;
        hint = fmt::format("starting to detect {} hotkey", dsn::enum_to_string(_hotkey_type));
        break;
    default:
        hint = "invalid collector state";
        resp.err = dsn::ERR_INVALID_STATE;
        resp.__set_err_hint(hint);
        LOG_ERROR_PREFIX(hint);
        CHECK(false, "invalid collector state");
    }
    resp.__set_err_hint(hint);
    LOG_WARNING_PREFIX(hint);
}

void hotkey_collector::on_stop_detect(dsn::replication::detect_hotkey_response &resp)
{
    change_state_to_stopped();
    resp.err = dsn::ERR_OK;
    std::string hint =
        fmt::format("{} hotkey stopped, cache cleared", dsn::enum_to_string(_hotkey_type));
    LOG_INFO_PREFIX(hint);
}

void hotkey_collector::query_result(dsn::replication::detect_hotkey_response &resp)
{
    if (_state != hotkey_collector_state::FINISHED) {
        resp.err = dsn::ERR_BUSY;
        std::string hint =
            fmt::format("Can't get hotkey now, now state: {}", enum_to_string(_state.load()));
        resp.__set_err_hint(hint);
        LOG_INFO_PREFIX(hint);
    } else {
        resp.err = dsn::ERR_OK;
        // Hot key should not be encrypted, thus use `c_escape_string` instead of
        // `c_escape_sensitive_string` (otherwise it would be overwritten with
        // "<redacted>").
        resp.__set_hotkey_result(pegasus::utils::c_escape_string(_result.hot_hash_key));
    }
}

bool hotkey_collector::terminate_if_timeout()
{
    if (dsn_now_s() >= _collector_start_time_second.load() + FLAGS_max_seconds_to_detect_hotkey) {
        LOG_INFO_PREFIX("hotkey collector work time is exhausted but no hotkey has been found");
        change_state_to_stopped();
        return true;
    }
    return false;
}

hotkey_coarse_data_collector::hotkey_coarse_data_collector(replica_base *base,
                                                           uint32_t hotkey_buckets_num)
    : internal_collector_base(base),
      _hash_bucket_num(hotkey_buckets_num),
      _hash_buckets(hotkey_buckets_num)
{
    for (auto &bucket : _hash_buckets) {
        bucket.store(0);
    }
}

void hotkey_coarse_data_collector::capture_data(const dsn::blob &hash_key, uint64_t weight)
{
    _hash_buckets[get_bucket_id(hash_key.to_string_view(), _hash_bucket_num)].fetch_add(weight);
}

void hotkey_coarse_data_collector::analyse_data(detect_hotkey_result &result)
{
    std::vector<uint64_t> buckets(_hash_bucket_num);
    for (int i = 0; i < buckets.size(); i++) {
        buckets[i] = _hash_buckets[i].load();
        _hash_buckets[i].store(0);
    }
    if (!find_outlier_index(
            buckets, FLAGS_hot_bucket_variance_threshold, result.coarse_bucket_index)) {
        result.coarse_bucket_index = -1;
    }
}

void hotkey_coarse_data_collector::clear()
{
    for (int i = 0; i < _hash_bucket_num; i++) {
        _hash_buckets[i].store(0);
    }
}

hotkey_fine_data_collector::hotkey_fine_data_collector(replica_base *base,
                                                       uint32_t hotkey_buckets_num,
                                                       uint32_t max_queue_size)
    : internal_collector_base(base),
      _max_queue_size(max_queue_size),
      _capture_key_queue(max_queue_size),
      _hash_bucket_num(hotkey_buckets_num)

{
    _target_bucket_index.store(-1);
}

void hotkey_fine_data_collector::change_target_bucket(int target_bucket_index)
{
    _target_bucket_index.store(target_bucket_index);
}

void hotkey_fine_data_collector::capture_data(const dsn::blob &hash_key, uint64_t weight)
{
    if (get_bucket_id(hash_key.to_string_view(), _hash_bucket_num) != _target_bucket_index.load()) {
        return;
    }
    // abandon the key if enqueue failed (possibly because not enough room to enqueue)
    _capture_key_queue.try_enqueue(std::make_pair(hash_key, weight));
}

struct blob_hash
{
    std::size_t operator()(const dsn::blob &str) const
    {
        std::string_view cp = str.to_string_view();
        return boost::hash_range(cp.begin(), cp.end());
    }
};

struct blob_equal
{
    std::size_t operator()(const dsn::blob &lhs, const dsn::blob &rhs) const
    {
        return lhs.to_string_view() == rhs.to_string_view();
    }
};

void hotkey_fine_data_collector::analyse_data(detect_hotkey_result &result)
{
    // hashkey -> weight
    std::unordered_map<dsn::blob, uint64_t, blob_hash, blob_equal> hash_keys_weight;
    std::pair<dsn::blob, uint64_t> key_weight_pair;
    // prevent endless loop, limit the number of elements analyzed not to exceed the queue size
    uint32_t dequeue_cnt = 0;
    while (++dequeue_cnt <= _max_queue_size && _capture_key_queue.try_dequeue(key_weight_pair)) {
        hash_keys_weight[key_weight_pair.first] += key_weight_pair.second;
    }

    if (hash_keys_weight.empty()) {
        return;
    }

    // the weight of all the collected hash keys
    std::vector<uint64_t> weights;
    weights.reserve(hash_keys_weight.size());
    std::string_view weight_max_key; // the hashkey with the max weight
    uint64_t weight_max = 0;         // the max weight by far
    for (const auto &iter : hash_keys_weight) {
        weights.push_back(iter.second);
        if (iter.second > weight_max) {
            weight_max = iter.second;
            weight_max_key = iter.first.to_string_view();
        }
    }

    // hash_key_counts stores the number of occurrences of each string captured in a period of
    // time The size of weights influences our hotkey determination strategy weights.size() <=
    // 2: the hotkey must exist (the most weighted key), because
    //                      the two-level filtering significantly reduces the
    //                      possibility that the hottest key is not the actual hotkey.
    // weights.size() >= 3: use find_outlier_index to determine whether a hotkey exists
    int hot_index;
    if (weights.size() < 3 ||
        find_outlier_index(weights, FLAGS_hot_key_variance_threshold, hot_index)) {
        result.hot_hash_key = std::string(weight_max_key);
    }
}

void hotkey_fine_data_collector::clear()
{
    _target_bucket_index.store(-1);
    std::pair<dsn::blob, uint64_t> key_weight_pair;
    while (_capture_key_queue.try_dequeue(key_weight_pair)) {
    }
}

} // namespace server
} // namespace pegasus

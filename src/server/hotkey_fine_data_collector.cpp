// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "hotkey_collector.h"

#include <dsn/tool-api/task.h>
#include <dsn/dist/replication/replication.codes.h>
#include <boost/functional/hash.hpp>

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

namespace pegasus {
namespace server {

const int kMaxQueueSize = 1000;

DSN_DEFINE_int32("pegasus.server",
                 fine_data_variance_threshold,
                 3,
                 "the threshold of variance calculate to find the outliers");

hotkey_fine_data_collector::hotkey_fine_data_collector(replica_base *base,
                                                       int hot_bucket,
                                                       dsn::apps::hotkey_type::type hotkey_type)
    : replica_base(base), _target_bucket(hot_bucket)
{
    // Distinguish between single-threaded and multi-threaded environments
    if (hotkey_type == dsn::apps::hotkey_type::READ) {
        auto threads = dsn::get_threadpool_threads_info(THREAD_POOL_LOCAL_APP);
        _thread_queue_map.reset(new std::unordered_map<int, int>);
        int queue_num = threads.size();
        for (int i = 0; i < queue_num; i++) {
            _thread_queue_map->insert({threads[i]->native_tid(), i});
        }
        _string_capture_queue.reserve(queue_num);
        for (int i = 0; i < queue_num; i++) {
            // Create a vector of the ReaderWriterQueue
            _string_capture_queue.emplace_back(kMaxQueueSize);
        }
    } else { // WRITE
        _string_capture_queue.emplace_back(kMaxQueueSize);
        _thread_queue_map.reset();
    }
}

inline int hotkey_fine_data_collector::get_queue_index()
{
    if (_thread_queue_map == nullptr) {
        return 0;
    }
    int thread_native_tid = ::dsn::utils::get_current_tid();
    auto result = _thread_queue_map->find(thread_native_tid);
    dassert(result != _thread_queue_map->end(), "Can't find the queue corresponding to the thread");
    return result->second;
}

void hotkey_fine_data_collector::capture_data(const dsn::blob &hash_key, uint64_t size)
{
    if (hotkey_collector::get_bucket_id(hash_key) != _target_bucket) {
        return;
    }
    _string_capture_queue[get_queue_index()].try_emplace(std::make_pair(hash_key, size));
}

bool hotkey_fine_data_collector::analyse_data(std::string &result)
{
    std::unordered_map<dsn::blob, uint64_t, blob_hash, blob_equal> hash_key_accessed_cnt;
    for (auto &rw_queue : _string_capture_queue) {
        std::pair<dsn::blob, int> hash_key_pair;
        // prevent endless loop
        int collect_sum = 0;
        while (rw_queue.try_dequeue(hash_key_pair) && collect_sum < kMaxQueueSize) {
            collect_sum++;
            hash_key_accessed_cnt[hash_key_pair.first] += hash_key_pair.second;
        }
    }
    if (hash_key_accessed_cnt.empty()) {
        return false;
    }
    std::vector<uint64_t> counts;
    counts.reserve(FLAGS_data_capture_hash_bucket_num);
    dsn::string_view count_max_key;
    uint64_t count_max = 0;
    for (const auto &iter : hash_key_accessed_cnt) {
        counts.push_back(iter.second);
        if (iter.second > count_max) {
            count_max = iter.second;
            count_max_key = iter.first; // the key with the max accessed count.
        }
    }
    // if the accessed counts differ hugely (depends on the variance threshold),
    // the max key is the hotkey.
    if (counts.size() < 3 ||
        hotkey_collector::variance_calc(counts, FLAGS_fine_data_variance_threshold) != -1) {
        result = std::string(count_max_key);
        return true;
    }
    return false;
}

} // namespace server
} // namespace pegasus

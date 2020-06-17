// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "hotkey_collector.h"

#include <dsn/tool-api/task.h>
#include <dsn/dist/replication/replication.codes.h>

namespace pegasus {
namespace server {

const int kMaxQueueSize = 1000;

DSN_DEFINE_int32("pegasus.server",
                 fine_data_variance_threshold,
                 3,
                 "the threshold of variance calculate to find the outliers");

hotkey_fine_data_collector::hotkey_fine_data_collector(hotkey_collector *base)
    : replica_base(base), _target_bucket(base->get_coarse_result())
{
    // Distinguish between single-threaded and multi-threaded environments
    if (base->get_hotkey_type() == dsn::apps::hotkey_type::READ) {
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
    } else {
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

void hotkey_fine_data_collector::capture_data(const std::string &data, int count)
{
    if (hotkey_collector::get_bucket_id(data) != _target_bucket)
        return;
    _string_capture_queue[get_queue_index()].try_emplace(std::make_pair(data, count));
}

bool hotkey_fine_data_collector::analyse_data(std::string &result)
{
    std::unordered_map<std::string, int> fine_data_bucket;
    for (int i = 0; i < _string_capture_queue.size(); i++) {
        std::pair<std::string, int> hash_key_pair;
        // prevent endless loop
        int collect_sum = 0;
        while (_string_capture_queue[i].try_dequeue(hash_key_pair) && collect_sum < kMaxQueueSize) {
            collect_sum++;
            fine_data_bucket[hash_key_pair.first] += hash_key_pair.second;
        }
    }
    if (fine_data_bucket.empty()) {
        return false;
    }
    std::vector<int> data_samples;
    data_samples.reserve(FLAGS_data_capture_hash_bucket_num);
    std::string count_max_key;
    int count_max = -1;
    for (const auto &iter : fine_data_bucket) {
        data_samples.push_back(iter.second);
        if (iter.second > count_max) {
            count_max = iter.second;
            count_max_key = iter.first;
        }
    }
    if (data_samples.size() < 3 ||
        hotkey_collector::variance_calc(data_samples, FLAGS_fine_data_variance_threshold) != -1) {
        result = count_max_key;
        return true;
    }
    derror_replica("Can't find a hot bucket in fine analyse");
    return false;
}

} // namespace server
} // namespace pegasus

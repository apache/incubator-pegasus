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

#include <dsn/utility/smart_pointers.h>
#include <dsn/utility/flags.h>
#include "base/pegasus_key_schema.h"

namespace pegasus {
namespace server {

DSN_DEFINE_int32("pegasus.server",
                 coarse_data_variance_threshold,
                 3,
                 "the threshold of variance calculate to find the outliers");

hotkey_collector::hotkey_collector()
    : _internal_collector(dsn::make_unique<hotkey_empty_data_collector>())
{
}

// TODO: (Tangyanzhao) implement these functions
void hotkey_collector::handle_rpc(const dsn::replication::detect_hotkey_request &req,
                                  dsn::replication::detect_hotkey_response &resp)
{
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

void hotkey_collector::analyse_data() { _internal_collector->analyse_data(); }

hotkey_coarse_data_collector::hotkey_coarse_data_collector(replica_base *base)
    : replica_base(base), _hash_buckets(FLAGS_data_capture_hash_bucket_num)
{
    for (std::atomic<uint64_t> &bucket : _hash_buckets) {
        bucket.store(0);
    }
}

void hotkey_coarse_data_collector::capture_data(const dsn::blob &hash_key, uint64_t weight)
{
    _hash_buckets[hotkey_collector::get_bucket_id(hash_key)].fetch_add(size);
}

void hotkey_coarse_data_collector::analyse_data()
{
    std::vector<uint64_t> buckets(FLAGS_data_capture_hash_bucket_num);
    for (int i = 0; i < buckets.size(); i++) {
        buckets[i] = _hash_buckets[i].load();
        _hash_buckets[i].store(0);
    }
    int result = variance_calc(buckets, FLAGS_coarse_data_variance_threshold);
    if (result >= 0) {
        return result;
    }
    derror_replica("Can't find a hot bucket in coarse analysis");
    return -1;
}

explicit hotkey_coarse_data_collector::hotkey_coarse_data_collector()
    : get_bucket_id(hotkey_collector::get_bucket_id),
      _hash_buckets(FLAGS_data_capture_hash_bucket_num)
{
    for (std::atomic<uint64_t> &bucket : _hash_buckets) {
        bucket.store(0);
    }
}

/*static*/ int hotkey_collector::get_bucket_id(dsn::string_view data)
{
    size_t hash_value = boost::hash_range(data.begin(), data.end());
    return static_cast<int>(hash_value % FLAGS_data_capture_hash_bucket_num);
}

detect_hotkey_result hotkey_collector::variance_calc(const std::vector<uint64_t> &data_samples,
                                                     int threshold)
{
    detect_hotkey_result result;
    int data_size = data_samples.size();
    double total = 0;
    int hot_index = 0;
    int hot_value = 0;
    for (int i = 0; i < data_size; i++) {
        total += data_samples[i];
        if (data_samples[i] > hot_value) {
            hot_index = i;
            hot_value = data_samples[i];
        }
    }
    // in case of sample size too small
    if (data_size < 3 || total < data_size) {
        derror("Data samples too small");
        return -1;
    }
    double avg = (total - data_samples[hot_index]) / (data_size - 1);
    double sd = 0;
    for (int j = 0; j < data_size; j++) {
        if (j != hot_index) {
            sd += pow((data_samples[j] - avg), 2);
        }
    }
    sd = sqrt(sd / (data_size - 2));
    double hot_point = (hot_value - avg) / sd;
    return hot_point > threshold ? hot_index : -1;
}

} // namespace server
} // namespace pegasus

// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "hotkey_collector.h"

namespace pegasus {
namespace server {

DSN_DEFINE_int32("pegasus.server",
                 coarse_data_variance_threshold,
                 3,
                 "the threshold of variance calculate to find the outliers");

hotkey_coarse_data_collector::hotkey_coarse_data_collector(replica_base *base)
    : replica_base(base), _hash_buckets(FLAGS_data_capture_hash_bucket_num)
{
    for (std::atomic<uint64_t> &bucket : _hash_buckets) {
        bucket.store(0);
    }
}

void hotkey_coarse_data_collector::capture_data(const dsn::blob &hash_key, uint64_t size)
{
    _hash_buckets[hotkey_collector::get_bucket_id(hash_key)].fetch_add(size);
}

int hotkey_coarse_data_collector::analyse_data()
{
    std::vector<uint64_t> buckets(FLAGS_data_capture_hash_bucket_num);
    for (int i = 0; i < buckets.size(); i++) {
        buckets[i] = _hash_buckets[i].load();
        _hash_buckets[i].store(0);
    }
    int result = hotkey_collector::variance_calc(buckets, FLAGS_coarse_data_variance_threshold);
    if (result >= 0) {
        return result;
    }
    derror_replica("Can't find a hot bucket in coarse analysis");
    return -1;
}

} // namespace server
} // namespace pegasus

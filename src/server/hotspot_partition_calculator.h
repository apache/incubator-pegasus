// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "hotspot_partition_data.h"

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <math.h>

#include <dsn/perf_counter/perf_counter.h>
#include "hotspot_partition_policy.h"

namespace pegasus {
namespace server {

// hotspot_partition_calculator is used to find the hotspot in Pegasus
class hotspot_partition_calculator
{
public:
    hotspot_partition_calculator(const std::string &app_name,
                                 const int partition_num,
                                 std::unique_ptr<hotspot_partition_policy> policy)
        : _app_name(app_name), _points(partition_num), _policy(std::move(policy))
    {
        init_perf_counter(partition_num);
    }
    void aggregate(const std::vector<row_data> &partitions);
    void start_alg();
    void init_perf_counter(const int perf_counter_count);

private:
    const std::string _app_name;
    std::vector<::dsn::perf_counter_wrapper> _points;
    std::queue<std::vector<hotspot_partition_data>> _app_data;
    std::unique_ptr<hotspot_partition_policy> _policy;
    static const int kMaxQueueSize = 100;

    FRIEND_TEST(hotspot_partition_calculator, hotspot_partition_policy);
};

} // namespace server
} // namespace pegasus

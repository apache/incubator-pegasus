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

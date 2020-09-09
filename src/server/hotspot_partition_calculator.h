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
#include <gtest/gtest_prod.h>
#include <dsn/perf_counter/perf_counter.h>
#include <dsn/utility/flags.h>

namespace pegasus {
namespace server {

// hotspot_partition_calculator is used to find the hotspot in Pegasus
class hotspot_partition_calculator
{
public:
    hotspot_partition_calculator(const std::string &app_name, const int partition_count)
        : _app_name(app_name), _hot_points(partition_count)
    {
        init_perf_counter(partition_count);
    }
    // aggregate related data of hotspot detection
    void data_aggregate(const std::vector<row_data> &partitions);
    // analyse the saved data to find hotspot partition
    void data_analyse();
    void init_perf_counter(const int perf_counter_count);

private:
    void _data_analyse(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                       std::vector<::dsn::perf_counter_wrapper> &perf_counters);
    const std::string _app_name;

    // usually _hot_points >= 3 can be considered as a hotspot partition
    std::vector<dsn::perf_counter_wrapper> _hot_points;
    // save historical data can improve accuracy
    std::queue<std::vector<hotspot_partition_data>> _historical_data;

    FRIEND_TEST(hotspot_partition_calculator, hotspot_partition_policy);
};

} // namespace server
} // namespace pegasus

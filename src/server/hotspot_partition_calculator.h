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

namespace pegasus {
namespace server {

// determines whether capture read data or write data on the server side
enum class hotkey_detect_type
{
    READ_HOTKEY_DETECT = 0,
    WRITE_HOTKEY_DETECT
};

// determines whether start or stop capture data on the server side
enum class hotkey_detect_action
{
    START_HOTKEY_DETECT = 0,
    STOP_HOTKEY_DETECT
};

// hotspot_partition_calculator is used to find the hot partition in a table.
class hotspot_partition_calculator
{
public:
    hotspot_partition_calculator(const std::string &app_name, int partition_count)
        : _app_name(app_name), _hot_points(partition_count)
    {
        init_perf_counter(partition_count);
    }
    // aggregate related data of hotspot detection
    void data_aggregate(const std::vector<row_data> &partitions);
    // analyse the saved data to find hotspot partition
    void data_analyse();
    static void send_hotkey_detect_request(const std::string &app_name,
                                           const uint64_t partition_index,
                                           const dsn::apps::hotkey_type::type hotkey_type,
                                           const dsn::apps::hotkey_detect_action::type action);

private:
    const std::string _app_name;

    void init_perf_counter(int perf_counter_count);
    // usually a partition with "hot-point value" >= 3 can be considered as a hotspot partition.
    std::vector<dsn::perf_counter_wrapper> _hot_points;
    // saving historical data can improve accuracy
    std::queue<std::vector<hotspot_partition_data>> _partition_stat_histories;

    FRIEND_TEST(hotspot_partition_calculator, hotspot_partition_policy);
};

} // namespace server
} // namespace pegasus

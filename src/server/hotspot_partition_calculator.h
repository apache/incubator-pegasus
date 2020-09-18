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

#include <gtest/gtest_prod.h>

#include <dsn/perf_counter/perf_counter.h>
#include <dsn/utility/flags.h>
#include "hotspot_partition_stat.h"

namespace pegasus {
namespace server {

DSN_DECLARE_int32(occurrence_threshold);

// stores the whole histories of all partitions in one table
typedef std::list<std::vector<hotspot_partition_stat>> stat_histories;
// hot_partition_counters c[index_of_partitions][type_of_read(0)/write(1)_stat]
// so if we have n partitions, we will get 2*n hot_partition_counters, to demonstrate both
// read/write hotspot value
typedef std::vector<std::array<dsn::perf_counter_wrapper, 2>> hot_partition_counters;

// hotspot_partition_calculator is used to find the hot partition in a table.
class hotspot_partition_calculator
{
public:
    hotspot_partition_calculator(const std::string &app_name, int partition_count)
        : _app_name(app_name), _hot_points(partition_count), _hotpartition_pool(partition_count)
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
    // empirical rule to calculate hot point of each partition
    // ref: https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule
    void stat_histories_analyse(int data_type, std::vector<int> &hot_points);
    // set hot_point to corresponding perf_counter
    void update_hot_point(int data_type, std::vector<int> &hot_points);
    void detect_hotkey_in_hotpartition(int data_type);

    const std::string _app_name;
    void init_perf_counter(int perf_counter_count);
    // usually a partition with "hot-point value" >= 3 can be considered as a hotspot partition.
    hot_partition_counters _hot_points;
    // saving historical data can improve accuracy
    stat_histories _partitions_stat_histories;

    std::vector<std::array<int, 2>> _hotpartition_pool;

    friend class hotspot_partition_test;
    FRIEND_TEST(hotspot_partition_test, send_hotkey_detect_request);
};

} // namespace server
} // namespace pegasus

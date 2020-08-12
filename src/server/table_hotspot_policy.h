// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "hotspot_partition_data.h"

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <math.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/perf_counter/perf_counter.h>
#include <dsn/dist/fmt_logging.h>

namespace pegasus {
namespace server {

const int kMaxQueueSize = 100;
typedef std::list<std::vector<hotspot_partition_data>> partition_data_list;
typedef std::vector<std::vector<std::unique_ptr<dsn::perf_counter_wrapper>>> hot_partition_counters;

enum hotkey_detect_type
{
    READ_HOTKEY_DETECT = 0,
    WRITE_HOTKEY_DETECT
};

enum hotkey_collector_operation
{
    START_HOTKEY_DETECT = 0,
    STOP_HOTKEY_DETECT
};

class hotspot_policy
{
public:
    // hotspot_app_data store the historical data which related to hotspot
    // it uses rolling queue to save all app's historical data
    // vector is used to save all the partitions' data of one app
    // hotspot_partition_data is used to save data of one partition
    virtual void analysis(const partition_data_list &hotspot_app_data,
                          hot_partition_counters &perf_counters) = 0;
};

// hotspot_calculator is used to find the hotspot in Pegasus
class hotspot_calculator
{
public:
    hotspot_calculator(const std::string &app_name,
                       const int partition_num,
                       std::unique_ptr<hotspot_policy> policy);
    void aggregate(const std::vector<row_data> &partitions);
    void start_alg();
    void init_perf_counter(const int perf_counter_count);
    static void notify_replica(const std::string &app_name,
                               const int partition_index,
                               const hotkey_detect_type type,
                               const hotkey_collector_operation operation);

private:
    const std::string _app_name;
    hot_partition_counters _hot_partition_points;
    std::vector<int> _over_threshold_times_read, _over_threshold_times_write;
    partition_data_list _app_data;
    std::unique_ptr<hotspot_policy> _policy;
    std::set<int> read_hot_partition;
    std::set<int> write_hot_partition;
    FRIEND_TEST(table_hotspot_policy, hotspot_algo_qps_variance);
};
} // namespace server
} // namespace pegasus

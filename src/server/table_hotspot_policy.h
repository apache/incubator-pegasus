// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "hotspot_partition_data.h"

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <dsn/perf_counter/perf_counter.h>

static const int MAX_STORE_SIZE = 100;

namespace pegasus {
namespace server {
class hotspot_policy
{
public:
    virtual void
    analysis_hotspot_data(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                          const std::vector<::dsn::perf_counter_wrapper> &hot_points) = 0;
};

class hotspot_algo_qps_skew : public hotspot_policy
{
public:
    void
    analysis_hotspot_data(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                          const std::vector<::dsn::perf_counter_wrapper> &hot_points)
    {
        std::queue<std::vector<hotspot_partition_data>> temp = hotspot_app_data;
        std::vector<hotspot_partition_data> anly_data;
        double min_total_qps = 1.0, min_total_cu = 1.0;
        for (int i = 0; i < temp.front().size(); i++) {
            anly_data.push_back(temp.front()[i]);
            std::cout << "data_anly :" << temp.front()[i].total_qps << std::endl;
            min_total_qps = std::min(min_total_qps, std::max(anly_data[i].total_qps, 1.0));
        }
        temp.pop();
        std::cout << anly_data.size() << " " << hot_points.size() << std::endl;

        dassert(anly_data.size() == hot_points.size(), "partittion counts error, please check");
        for (int i = 0; i < hot_points.size(); i++) {
            std::cout << "data :" << anly_data[i].total_qps << std::endl;
            hot_points[i]->set(anly_data[i].total_qps / min_total_qps);
        }
        return;
    }
};

class hotspot_calculator
{
public:
    hotspot_calculator(const std::string &app_name, const int &partition_num)
        : app_name(app_name), _hotpot_points(partition_num)
    {
        this->init_perf_counter();
    }
    void aggregate(const std::vector<row_data> &partitions);
    void start_alg();
    void init_perf_counter();

    std::queue<std::vector<hotspot_partition_data>> hotspot_app_data;
    const std::string app_name;

private:
    hotspot_policy *_policy;
    std::vector<::dsn::perf_counter_wrapper> _hotpot_points;
    FRIEND_TEST(table_hotspot_policy, hotspot_algo_qps_skew);
};
} // namespace pegasus
} // namespace server

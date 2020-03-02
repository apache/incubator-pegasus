// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "hotspot_partition_data.h"

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <math.h>

#include <dsn/perf_counter/perf_counter.h>

namespace pegasus {
namespace server {
class hotspot_policy
{
public:
    // hotspot_app_data store the historical data which related to hotspot
    // it uses rolling queue to save one app's data
    // vector is used to save the partitions' data of this app
    // hotspot_partition_data is used to save data of one partition
    virtual void analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                          std::vector<::dsn::perf_counter_wrapper> &hot_points) = 0;
};

class hotspot_algo_qps_skew : public hotspot_policy
{
public:
    void analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                  std::vector<::dsn::perf_counter_wrapper> &hot_points)
    {
        const auto &anly_data = hotspot_app_data.back();
        double min_total_qps = INT_MAX;
        for (auto partition_anly_data : anly_data) {
            min_total_qps = std::min(min_total_qps, partition_anly_data.total_qps);
        }
        min_total_qps = std::max(1.0, min_total_qps);
        dassert(anly_data.size() == hot_points.size(), "partition counts error, please check");
        for (int i = 0; i < hot_points.size(); i++) {
            hot_points[i]->set(anly_data[i].total_qps / min_total_qps);
        }
    }
};

// PauTa Criterion
class hotspot_algo_qps_variance : public hotspot_policy
{
public:
    void analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                  std::vector<::dsn::perf_counter_wrapper> &hot_points)
    {
        dassert(hotspot_app_data.back().size() == hot_points.size(),
                "partition counts error, please check");
        std::vector<double> data_samples;
        data_samples.reserve(hotspot_app_data.size() * hot_points.size());
        const auto &anly_data = hotspot_app_data.back();
        auto temp_data = hotspot_app_data;
        double avg = 0, sd = 0;
        int n = 0;
        // avg: Average number
        // sd: Standard deviation
        // n: Number of samples
        while (!temp_data.empty()) {
            for (auto partition_data : temp_data.front()) {
                if (partition_data.total_qps - 1.00 > 0) {
                    data_samples.push_back(partition_data.total_qps);
                    avg += partition_data.total_qps;
                    n++;
                }
            }
            temp_data.pop();
        }
        if (n == 0) {
            ddebug("hotspot_app_data size == 0");
            return;
        }
        avg /= n;
        for (auto data_sample : data_samples) {
            sd += pow((data_sample - avg), 2);
        }
        sd = sqrt(sd / n);
        for (int i = 0; i < hot_points.size(); i++) {
            double hot_point = (anly_data[i].total_qps - avg) / sd;
            // perf_counter->set can only be unsigned __int64
            // use ceil to guarantee conversion results
            hot_point = ceil(std::max(hot_point, double(0)));
            hot_points[i]->set(hot_point);
        }
    }
};

// hotspot_calculator is used to find the hotspot in Pegasus
class hotspot_calculator
{
public:
    hotspot_calculator(const std::string &app_name,
                       const int partition_num,
                       const hotspot_policy policy)
        : _app_name(app_name), _points(partition_num), _policy(policy)
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
    std::unique_ptr<hotspot_policy> _policy;
    static const int kMaxQueueSize = 100;

    FRIEND_TEST(table_hotspot_policy, hotspot_algo_qps_variance);
};
} // namespace server
} // namespace pegasus

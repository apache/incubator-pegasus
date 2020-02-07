// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "data_store.h"
#include <algorithm>
#include <dsn/perf_counter/perf_counter.h>

static const int MAX_STORE_SIZE = 100;

namespace pegasus {
namespace server {
class hotspot_policy
{
public:
    virtual void analysis_hotspot_data(std::vector<std::queue<data_store>> *data_stores,
                                       std::vector<double> *hot_points) = 0;
};

class hotspot_algo_qps_skew : public hotspot_policy
{
public:
    void analysis_hotspot_data(std::vector<std::queue<data_store>> *data_stores,
                               std::vector<double> *hot_points)
    {
        std::vector<std::queue<data_store>> temp = *data_stores;
        std::vector<data_store> anly_data;
        double min_total_qps = 1.0, min_total_cu = 1.0;
        for (int i = 0; i < temp.size(); i++) {
            anly_data.push_back(temp[i].back());
            min_total_qps = std::min(min_total_qps, std::max(anly_data[i].total_qps, 1.0));
        }
        for (int i = 0; i < anly_data.size(); i++) {
            (*hot_points)[i] = anly_data[i].total_qps / min_total_qps;
        }
        return;
    }
};

class hotspot_calculator
{
public:
    hotspot_calculator(const std::string &app_name, const int &partition_num)
        : data_stores(partition_num),
          app_name(app_name),
          _hotpot_point_value(partition_num),
          _hotpot_points(partition_num)
    {
    }
    // queue stores historical data of single partition's data_store(hotspots key value)
    // vector stores all partition's data of this app
    std::vector<std::queue<data_store>> data_stores;
    void aggregate(const std::vector<row_data> partitions);
    void start_alg();
    void init_perf_counter();
    void get_hotpot_point_value(std::vector<double> &result);
    void set_result_to_falcon();

    const std::string app_name;

private:
    hotspot_policy *_policy;
    std::vector<double> _hotpot_point_value;
    std::vector<::dsn::perf_counter_wrapper> _hotpot_points;
};
} // namespace pegasus
} // namespace server

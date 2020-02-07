// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "data_store.h"
#include <algorithm>
#include <dsn/perf_counter/perf_counter.h>

#define MAX_STORE_SIZE 100

namespace pegasus {
namespace server {
class Hotspot_policy
{
public:
    virtual void detect_hotspot_policy(std::vector<std::queue<data_store>> *data_stores,
                                       std::vector<double> *hot_points) = 0;
};

class Algo1 : public Hotspot_policy
{
public:
    void detect_hotspot_policy(std::vector<std::queue<data_store>> *data_stores,
                               std::vector<double> *hot_points)
    {
        std::vector<std::queue<data_store>> temp = *data_stores;
        std::vector<data_store> anly_data;
        double min_total_qps = 1.0, min_total_cu = 1.0;
        for (int index = 0; index < temp.size(); index++) {
            anly_data.push_back(temp[index].back());
            min_total_qps = std::min(min_total_qps, std::max(anly_data[index].total_qps, 1.0));
        }
        for (int i = 0; i < anly_data.size(); i++) {
            hot_points->at(i) = anly_data[i].total_qps / min_total_qps;
        }
        return;
    }
};

class hotspot_calculator
{
public:
    std::vector<std::queue<data_store>> data_stores;
    hotspot_calculator(const std::string &app_name, const int &app_size);
    void aggregate(const std::vector<row_data> partitions);
    void start_alg();
    void init_perf_counter();
    void get_hotpot_point_value(std::vector<double> &result);
    void set_result_to_falcon();
    const std::string app_name;

private:
    Hotspot_policy *_policy;
    std::vector<double> _hotpot_point_value;
    std::vector<::dsn::perf_counter_wrapper> _hotpot_points;
};
} // namespace pegasus
} // namespace server
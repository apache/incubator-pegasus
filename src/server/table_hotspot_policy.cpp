// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"

#include <assert.h>

namespace pegasus {
namespace server {

void hotspot_calculator::aggregate(const std::vector<row_data> partitions)
{
    for (int i = 0; i < partitions.size(); i++) {
        while (this->data_stores[i].size() > MAX_STORE_SIZE - 1) {
            this->data_stores[i].pop();
        }
        this->data_stores[i].emplace(data_store(partitions[i], this->app_name));
    }
}

void hotspot_calculator::init_perf_counter()
{
    char counter_name[1024];
    char counter_desc[1024];
    for (int i = 0; i < this->_hotpot_points.size(); i++) {
        string paritition_desc = this->app_name + std::to_string(i);
        sprintf(counter_name, "app.stat.hotspots.%s", paritition_desc.c_str());
        sprintf(counter_desc, "statistic the hotspots of app %s", paritition_desc.c_str());
        _hotpot_points[i].init_app_counter(
            "app.pegasus", counter_name, COUNTER_TYPE_NUMBER, counter_desc);
    }
}

void hotspot_calculator::get_hotpot_point_value(std::vector<double> &result)
{
    result.assign(this->_hotpot_point_value.begin(), this->_hotpot_point_value.end());
}

void hotspot_calculator::set_result_to_falcon()
{
    dassert(_hotpot_points.size() != _hotpot_point_value.size(),"partittion counts error, please check");
    for (int i = 0; i < _hotpot_points.size(); i++){
        _hotpot_points[i]->set(_hotpot_point_value[i]);
    }
}

void hotspot_calculator::start_alg()
{
    _policy = new Algo1();
    _policy->detect_hotspot_policy(&(this->data_stores), &(this->_hotpot_point_value));
}
} // namespace pegasus
} // namespace server
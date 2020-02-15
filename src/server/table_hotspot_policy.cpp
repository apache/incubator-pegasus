// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"

#include <dsn/dist/fmt_logging.h>

namespace pegasus {
namespace server {

void hotspot_calculator::aggregate(const std::vector<row_data> &partitions)
{
    while (hotspot_app_data.size() > MAX_STORE_SIZE - 1) {
        hotspot_app_data.pop();
    }
    std::vector<hotspot_partition_data> temp(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
        temp.[i] = std::move(hotspot_partition_data(partition));
    }
    hotspot_app_data.emplace(temp);
}

void hotspot_calculator::init_perf_counter(const int perf_counter_count)
{
    std::string counter_name;
    std::string counter_desc;
    for (int i = 0; i < perf_counter_count; i++) {
        string paritition_desc = app_name + '@' + std::to_string(i);
        counter_name = fmt::format("app.stat.hotspots.{}", paritition_desc.c_str());
        counter_desc = fmt::format("statistic the hotspots of app {}", paritition_desc.c_str());
        _hotpot_points[i].init_app_counter(
            "app.pegasus", counter_name.c_str(), COUNTER_TYPE_NUMBER, counter_desc.c_str());
    }
}

void hotspot_calculator::start_alg(const std::shared_ptr<hotspot_policy> hotspot_algo)
{
    hotspot_algo->analysis_hotspot_data(hotspot_app_data, _hotpot_points);
}

} // namespace server
} // namespace pegasus

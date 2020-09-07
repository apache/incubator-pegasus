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

#include "hotspot_partition_calculator.h"

#include <dsn/dist/fmt_logging.h>

namespace pegasus {
namespace server {

void hotspot_partition_calculator::aggregate(const std::vector<row_data> &partitions)
{
    while (_app_data.size() > kMaxQueueSize - 1) {
        _app_data.pop();
    }
    std::vector<hotspot_partition_data> temp(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
        temp[i] = std::move(hotspot_partition_data(partitions[i]));
    }
    _app_data.emplace(temp);
}

void hotspot_partition_calculator::init_perf_counter(const int perf_counter_count)
{
    std::string counter_name;
    std::string counter_desc;
    for (int i = 0; i < perf_counter_count; i++) {
        string paritition_desc = _app_name + '.' + std::to_string(i);
        counter_name = fmt::format("app.stat.hotspots@{}", paritition_desc);
        counter_desc = fmt::format("statistic the hotspots of app {}", paritition_desc);
        _points[i].init_app_counter(
            "app.pegasus", counter_name.c_str(), COUNTER_TYPE_NUMBER, counter_desc.c_str());
    }
}

void hotspot_partition_calculator::start_alg() { _policy->analysis(_app_data, _points); }

} // namespace server
} // namespace pegasus

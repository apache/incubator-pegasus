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

#include "hotspot_partition_policy.h"

namespace pegasus {
namespace server {

void hotspot_partition_policy::analysis(
    const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
    std::vector<::dsn::perf_counter_wrapper> &perf_counters)
{
    dassert(hotspot_app_data.back().size() == perf_counters.size(),
            "partition counts error, please check");
    std::vector<double> data_samples;
    data_samples.reserve(hotspot_app_data.size() * perf_counters.size());
    auto temp_data = hotspot_app_data;
    double total = 0, sd = 0, avg = 0;
    int sample_count = 0;
    // avg: Average number
    // sd: Standard deviation
    // sample_count: Number of samples
    while (!temp_data.empty()) {
        for (auto partition_data : temp_data.front()) {
            if (partition_data.total_qps - 1.00 > 0) {
                data_samples.push_back(partition_data.total_qps);
                total += partition_data.total_qps;
                sample_count++;
            }
        }
        temp_data.pop();
    }
    if (sample_count == 0) {
        ddebug("hotspot_app_data size == 0");
        return;
    }
    avg = total / sample_count;
    for (auto data_sample : data_samples) {
        sd += pow((data_sample - avg), 2);
    }
    sd = sqrt(sd / sample_count);
    const auto &anly_data = hotspot_app_data.back();
    for (int i = 0; i < perf_counters.size(); i++) {
        double hot_point = (anly_data[i].total_qps - avg) / sd;
        // perf_counter->set can only be unsigned __int64
        // use ceil to guarantee conversion results
        hot_point = ceil(std::max(hot_point, double(0)));
        perf_counters[i]->set(hot_point);
    }
}

} // namespace server
} // namespace pegasus

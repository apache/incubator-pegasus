// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "hotspot_algo_qps_variance.h"

namespace pegasus {
namespace server {

void hotspot_algo_qps_variance::pauta_analysis(const partition_data_list &hotspot_app_data,
                                               hot_partition_counters &perf_counters,
                                               partition_qps_type data_type)
{
    dcheck_eq(hotspot_app_data.back().size(), perf_counters.size());
    // avg: Average number
    // sd: Standard deviation
    // sample_count: Number of samples
    double total = 0, sd = 0, avg = 0;
    int sample_count = 0;
    std::vector<double> data_samples;
    data_samples.reserve(hotspot_app_data.size() * perf_counters.size());
    for (const auto &partition_datas : hotspot_app_data) {
        for (const auto &partition_data : partition_datas) {
            if (partition_data.total_qps[data_type] > 1.00) {
                data_samples.push_back(partition_data.total_qps[data_type]);
                total += partition_data.total_qps[data_type];
                sample_count++;
            }
        }
    }

    if (sample_count <= 1) {
        return;
    }
    avg = total / sample_count;
    for (auto data_sample : data_samples) {
        sd += pow((data_sample - avg), 2);
    }
    sd = sqrt(sd / (sample_count - 1));
    const auto &anly_data = hotspot_app_data.back();
    for (int i = 0; i < perf_counters.size(); i++) {
        double hot_point = 0;
        if (sd != 0) {
            hot_point = (anly_data[i].total_qps[data_type] - avg) / sd;
        }
        // perf_counter->set can only be unsigned __int64
        // use ceil to guarantee conversion results
        hot_point = ceil(std::max(hot_point, double(0)));
        perf_counters[i][data_type]->get()->set(hot_point);
    }
}

void hotspot_algo_qps_variance::analysis(const partition_data_list &hotspot_app_data,
                                         hot_partition_counters &perf_counters)
{
    pauta_analysis(hotspot_app_data, perf_counters, READ_HOTSPOT_DATA);
    pauta_analysis(hotspot_app_data, perf_counters, WRITE_HOTSPOT_DATA);
}
} // namespace server
} // namespace pegasus

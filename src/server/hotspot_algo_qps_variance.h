// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <math.h>

#include "hotspot_partition_data.h"
#include "table_hotspot_policy.h"
#include <dsn/perf_counter/perf_counter.h>

namespace pegasus {
namespace server {
// PauTa Criterion
class hotspot_algo_qps_variance : public hotspot_policy
{
public:
    void analysis(const partition_data_list &hotspot_app_data,
                  hot_partition_counters &perf_counters);

private:
    void pauta_analysis(const partition_data_list &hotspot_app_data,
                        hot_partition_counters &perf_counters,
                        partition_qps_type data_type);
};
} // namespace server
} // namespace pegasus

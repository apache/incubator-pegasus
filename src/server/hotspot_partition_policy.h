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

#pragma once

#include "hotspot_partition_data.h"

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <math.h>

#include <dsn/perf_counter/perf_counter.h>

namespace pegasus {
namespace server {
// PauTa Criterion
class hotspot_partition_policy
{
public:
    void analysis(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                  std::vector<::dsn::perf_counter_wrapper> &perf_counters);
};

} // namespace server
} // namespace pegasus

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

#include <string>
#include "common/json_helper.h"
#include "perf_counter/perf_counter.h"

namespace dsn {

struct perf_counter_metric
{
    std::string name;
    std::string type;
    double value;
    perf_counter_metric() : value(0) {}
    perf_counter_metric(const char *n, dsn_perf_counter_type_t t, double v)
        : name(n), type(dsn_counter_type_to_string(t)), value(v)
    {
    }
    DEFINE_JSON_SERIALIZATION(name, type, value)
};

/// used for command of querying perf counter
struct perf_counter_info
{
    std::string result; // OK or ERROR
    int64_t timestamp;  // in seconds
    std::string timestamp_str;
    std::vector<perf_counter_metric> counters;
    perf_counter_info() : timestamp(0) {}
    DEFINE_JSON_SERIALIZATION(result, timestamp, timestamp_str, counters)
};

} // namespace dsn

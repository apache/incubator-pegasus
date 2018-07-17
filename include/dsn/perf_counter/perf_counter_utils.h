// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <dsn/cpp/json_helper.h>
#include <dsn/perf_counter/perf_counter.h>

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

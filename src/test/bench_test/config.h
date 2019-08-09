// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <dsn/utility/config_api.h>
#include <rocksdb/env.h>

namespace pegasus {
namespace test {

/** Thread safety singleton */
struct config
{
    static config *get_instance();

    /** config parameters */
    std::string pegasus_cluster_name;
    std::string pegasus_app_name;
    uint32_t pegasus_timeout_ms;
    std::string benchmarks;
    uint32_t num;
    uint32_t seed;
    uint32_t threads;
    uint32_t duration_seconds;
    uint32_t value_size;
    uint32_t _batch_size;
    uint32_t _key_size;
    double _compression_ratio;
    uint32_t _ops_between_duration_checks;
    uint64_t _stats_interval;
    uint32_t _stats_interval_seconds;
    uint64_t _report_interval_seconds;
    std::string _report_file;
    uint32_t _thread_status_per_interval;
    uint32_t _prefix_size;
    uint32_t _keys_per_prefix;
    rocksdb::Env *_env;

private:
    config();
};
} // namespace test
} // namespace pegasus

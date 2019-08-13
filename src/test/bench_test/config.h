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
    uint32_t threads;
    uint32_t value_size;
    uint32_t batch_size;
    uint32_t key_size;
    uint64_t stats_interval;
    uint32_t stats_interval_seconds;
    uint64_t report_interval_seconds;
    std::string report_file;
    uint32_t thread_status_per_interval;
    uint32_t prefix_size;
    uint32_t keys_per_prefix;
    rocksdb::Env *env;

private:
    config();
};
} // namespace test
} // namespace pegasus

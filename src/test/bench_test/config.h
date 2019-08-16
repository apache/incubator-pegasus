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
    static const config &get_instance();

    std::string pegasus_cluster_name;
    std::string pegasus_app_name;
    // Pegasus read/write/delete timeout in milliseconds
    uint32_t pegasus_timeout_ms;
    // Comma-separated list of operations to run
    std::string benchmarks;
    // Number of key/values to place in database
    uint64_t num;
    // Number of concurrent threads to run
    uint32_t threads;
    uint32_t value_size;
    uint32_t hashkey_size;
    uint32_t sortkey_size;
    // Takes and report a snapshot of the current status of each thread when this is greater than 0
    uint32_t thread_status_per_interval;
    // Default environment suitable for the current operating system
    rocksdb::Env *env;

private:
    config();
};
} // namespace test
} // namespace pegasus

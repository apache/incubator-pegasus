// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <rocksdb/env.h>
#include <dsn/utility/singleton.h>

namespace pegasus {
namespace test {
/** Thread safety singleton */
struct config : public ::dsn::utils::singleton<config>
{
    config();

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
    // size of each value
    uint32_t value_size;
    // size of each hashkey
    uint32_t hashkey_size;
    // size of each sortkey
    uint32_t sortkey_size;
    // Seed base for random number generators
    uint64_t seed;
    // Default environment suitable for the current operating system
    rocksdb::Env *env;
};
} // namespace test
} // namespace pegasus

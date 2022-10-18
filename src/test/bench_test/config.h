/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <string>
#include <rocksdb/env.h>
#include "utils/singleton.h"

namespace pegasus {
namespace test {
/** Thread safety singleton */
struct config : public dsn::utils::singleton<config>
{
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

private:
    config();
    ~config() = default;

    friend class dsn::utils::singleton<config>;
};
} // namespace test
} // namespace pegasus

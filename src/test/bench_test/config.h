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

#include "utils/singleton.h"

namespace rocksdb {
class Env;
} // namespace rocksdb

namespace pegasus {
namespace test {
/** Thread safety singleton */
struct config : public dsn::utils::singleton<config>
{
    // Comma-separated list of operations to run
    std::string benchmarks;
    // Default environment suitable for the current operating system
    rocksdb::Env *env;

private:
    config();
    ~config() = default;

    friend class dsn::utils::singleton<config>;
};
} // namespace test
} // namespace pegasus

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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <string>

namespace pegasus {
namespace test {
thread_local std::ranlux48_base thread_local_rng(std::random_device{}());

void reseed_thread_local_rng(uint64_t seed) { thread_local_rng.seed(seed); }

uint64_t next_u64()
{
    return std::uniform_int_distribution<uint64_t>(0, std::numeric_limits<uint64_t>::max())(
        thread_local_rng);
}

std::string generate_string(uint64_t len)
{
    std::string key;

    // fill with random int
    uint64_t random_int = next_u64();
    key.append(reinterpret_cast<char *>(&random_int), std::min<uint64_t>(len, 8UL));

    // append with '0'
    key.resize(len, '0');

    return key;
}
} // namespace test
} // namespace pegasus

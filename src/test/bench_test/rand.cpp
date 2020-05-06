// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <random>

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
    key.append(reinterpret_cast<char *>(&random_int), std::min(len, 8UL));

    // append with '0'
    key.resize(len, '0');

    return key;
}
} // namespace test
} // namespace pegasus

// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/rand.h>
#include <random>

namespace dsn {
namespace rand {

thread_local std::ranlux48_base g_thread_local_rng(std::random_device{}());

/*extern*/ uint64_t next_u64(uint64_t min, uint64_t max)
{
    return std::uniform_int_distribution<uint64_t>(min, max)(g_thread_local_rng);
}

/*extern*/ void reseed_thread_local_rng(uint64_t seed) { g_thread_local_rng.seed(seed); }

} // namespace rand
} // namespace dsn

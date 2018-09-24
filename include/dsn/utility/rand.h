// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>

namespace dsn {
namespace rand {

/// This package offers several functions for random number generation.
/// It is guaranteed to be thread-safe by using a PRNG with one instance per thread.
/// By default, the RNG is seeded from std::random_device.

/// \returns, as an uint64_t, a non-negative pseudo-random number in [min, max].
extern uint64_t next_u64(uint64_t min, uint64_t max);

/// \returns, as an uint64_t, a non-negative pseudo-random number in [0, n).
/// If n == 0, it returns 0.
inline uint64_t next_u64(uint64_t n)
{
    if (n == 0)
        return 0;
    return next_u64(0, n - 1);
}

/// \returns a pseudo-random 64-bit value as a uint64_t.
inline uint64_t next_u64() { return next_u64(0, std::numeric_limits<uint64_t>::max()); }

/// \returns, as an uint32_t, a non-negative pseudo-random number in [min, max].
inline uint32_t next_u32(uint32_t min, uint32_t max)
{
    return static_cast<uint32_t>(next_u64(min, max));
}

/// \returns, as an uint32_t, a non-negative pseudo-random number in [0, n).
/// If n == 0, it returns 0.
inline uint32_t next_u32(uint32_t n) { return static_cast<uint32_t>(next_u64(n)); }

/// \returns a pseudo-random 32-bit value as a uint32_t.
inline uint32_t next_u32() { return next_u32(0, std::numeric_limits<uint32_t>::max()); }

/// \returns, as a double, a pseudo-random number in [0.0,1.0].
inline double next_double01() { return next_u64(0, 1000000000) / 1000000000.0; }

/// Reseeds the RNG of current thread.
extern void reseed_thread_local_rng(uint64_t seed);

} // namespace rand
} // namespace dsn

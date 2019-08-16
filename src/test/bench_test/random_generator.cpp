// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "random_generator.h"

namespace pegasus {
namespace test {
uint32_t random_generator::uniform(int n) { return next() % n; }

uint32_t random_generator::next() { return dsn::rand::next_u32(); }

void random_generator::reseed(uint64_t seed) { dsn::rand::reseed_thread_local_rng(seed); }
} // namespace test
} // namespace pegasus

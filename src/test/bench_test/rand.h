// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

namespace pegasus {
namespace test {
// Reseeds the RNG of current thread.
extern void reseed_thread_local_rng(uint64_t seed);
extern uint64_t next_u64();
extern std::string generate_string(uint64_t len);
}
}

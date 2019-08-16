// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/rand.h>

namespace pegasus {
namespace test {
class random_generator
{
public:
    static uint32_t uniform(int n);
    static uint32_t next();
    static void reseed(uint64_t seed);
};
} // namespace test
} // namespace pegasus

// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <cstdint>
#include "config.h"

namespace pegasus {
namespace test {
class duration
{
public:
    duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0);
    int64_t get_stage();
    bool done(int64_t increment);

private:
    uint64_t _max_seconds;
    int64_t _max_ops;
    int64_t _ops_per_stage;
    int64_t _ops;
    uint64_t _start_at;
};
}
}

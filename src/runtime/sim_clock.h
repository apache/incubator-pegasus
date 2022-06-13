// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/clock.h>
#include "scheduler.h"

namespace dsn {
namespace tools {

class sim_clock : public utils::clock
{
public:
    sim_clock() = default;
    virtual ~sim_clock() = default;

    // Gets simulated time in nanoseconds.
    virtual uint64_t now_ns() const { return scheduler::instance().now_ns(); }
};

} // namespace tools
} // namespace dsn

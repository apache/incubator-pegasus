// Created by mi on 2019/8/7.
// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <algorithm>
#include "duration.h"

namespace pegasus {
namespace test {

duration::duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage)
{
    _max_seconds = max_seconds;
    _max_ops = max_ops;
    _ops_per_stage = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    _ops = 0;
    _start_at = config::get_instance()->_env->NowMicros();
}

int64_t duration::get_stage() { return std::min(_ops, _max_ops - 1) / _ops_per_stage; }

bool duration::done(int64_t increment)
{
    if (increment <= 0)
        increment = 1; // avoid Done(0) and infinite loops
    _ops += increment;

    if (_max_seconds) {
        // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
        auto granularity = config::get_instance()->_ops_between_duration_checks;
        if ((_ops / granularity) != ((_ops - increment) / granularity)) {
            uint64_t now = config::get_instance()->_env->NowMicros();
            return ((now - _start_at) / 1000000) >= _max_seconds;
        } else {
            return false;
        }
    } else {
        return _ops > _max_ops;
    }
}
} // namespace test
} // namespace pegasus

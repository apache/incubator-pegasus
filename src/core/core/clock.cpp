// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/clock.h>
#include <dsn/utility/time_utils.h>
#include <dsn/utility/dlib.h>
#include <dsn/utility/smart_pointers.h>

DSN_API uint64_t dsn_now_ns() { return dsn::utils::clock::instance()->now_ns(); }

namespace dsn {
namespace utils {

std::unique_ptr<clock> clock::_clock = make_unique<clock>();

const clock *clock::instance() { return _clock.get(); }

uint64_t clock::now_ns() const { return get_current_physical_time_ns(); }

void clock::mock(clock *mock_clock) { _clock.reset(mock_clock); }

} // namespace utils
} // namespace dsn

// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <memory>

namespace dsn {
namespace utils {

class clock
{
public:
    clock() = default;
    virtual ~clock() = default;

    // Gets current time in nanoseconds.
    virtual uint64_t now_ns() const;

    // Gets singleton instance. eager singleton, which is thread safe
    static const clock *instance();

    // Resets the global clock implementation (not thread-safety)
    static void mock(clock *mock_clock);

private:
    static std::unique_ptr<clock> _clock;
};

} // namespace utils
} // namespace dsn

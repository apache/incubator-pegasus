/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <chrono>

/// This is a simple implementation of chrono literals
/// (http://en.cppreference.com/w/cpp/chrono/duration#Literals).
/// Deprecate this when we have our compiler version updated to gcc-5
/// (https://en.cppreference.com/w/cpp/compiler_support).

namespace dsn {

/// Example:
///
///   using namespace dsn::literals::chrono_literals;
///
///   template <typename F>
///   void schedule(F &&f, std::chrono::milliseconds delay_ms = 0_ms);
///

inline namespace literals {
inline namespace chrono_literals {

constexpr std::chrono::hours operator"" _h(unsigned long long v) { return std::chrono::hours{v}; }

constexpr std::chrono::minutes operator"" _min(unsigned long long v)
{
    return std::chrono::minutes{v};
}

constexpr std::chrono::seconds operator"" _s(unsigned long long v)
{
    return std::chrono::seconds{v};
}

constexpr std::chrono::milliseconds operator"" _ms(unsigned long long v)
{
    return std::chrono::milliseconds{v};
}

constexpr std::chrono::microseconds operator"" _us(unsigned long long v)
{
    return std::chrono::microseconds{v};
}

constexpr std::chrono::nanoseconds operator"" _ns(unsigned long long v)
{
    return std::chrono::nanoseconds{v};
}

} // namespace chrono_literals
} // namespace literals
} // namespace dsn

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

#include <climits>

#include <dsn/utility/string_view.h>

namespace dsn {

namespace internal {

template <typename T>
bool buf2signed(string_view buf, T &result)
{
    static_assert(std::is_signed<T>::value, "buf2signed works only with signed integer");

    if (buf.empty()) {
        return false;
    }

    const int saved_errno = errno;
    errno = 0;
    char *p = nullptr;
    long long v = std::strtoll(buf.data(), &p, 10);

    if (p - buf.data() != buf.length()) {
        return false;
    }

    if (v > std::numeric_limits<T>::max() || v < std::numeric_limits<T>::min() || errno != 0) {
        return false;
    }

    if (errno == 0) {
        errno = saved_errno;
    }

    result = v;
    return true;
}

} // namespace internal

bool buf2int32(string_view buf, int32_t &result) { return internal::buf2signed(buf, result); }

bool buf2int64(string_view buf, int64_t &result) { return internal::buf2signed(buf, result); }

bool buf2bool(string_view buf, bool &result)
{
    if (buf == "true") {
        result = true;
        return true;
    }
    if (buf == "false") {
        result = false;
        return true;
    }
    return false;
}

} // namespace dsn

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
#include <cmath>

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

    std::string str(buf.data(), buf.length());
    const int saved_errno = errno;
    errno = 0;
    char *p = nullptr;
    long long v = std::strtoll(str.data(), &p, 0);

    if (p - str.data() != str.length()) {
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

template <typename T>
bool buf2unsigned(string_view buf, T &result)
{
    static_assert(std::is_unsigned<T>::value, "buf2unsigned works only with unsigned integer");

    if (buf.empty()) {
        return false;
    }

    std::string str(buf.data(), buf.length());
    const int saved_errno = errno;
    errno = 0;
    char *p = nullptr;
    unsigned long long v = std::strtoull(str.data(), &p, 0);

    if (p - str.data() != str.length()) {
        return false;
    }

    if (v > std::numeric_limits<T>::max() || v < std::numeric_limits<T>::min() || errno != 0) {
        return false;
    }

    if (errno == 0) {
        errno = saved_errno;
    }

    // strtoull() will convert a negative integer to an unsigned integer,
    // return false in this condition. (but we consider "-0" is correct)
    if (v != 0 && str.find('-') != std::string::npos) {
        return false;
    }

    result = v;
    return true;
}
} // namespace internal

/// buf2*: `result` will keep unmodified if false is returned.

inline bool buf2int32(string_view buf, int32_t &result)
{
    return internal::buf2signed(buf, result);
}

inline bool buf2int64(string_view buf, int64_t &result)
{
    return internal::buf2signed(buf, result);
}

inline bool buf2uint64(string_view buf, uint64_t &result)
{
    return internal::buf2unsigned(buf, result);
}

inline bool buf2bool(string_view buf, bool &result, bool ignore_case = true)
{
    std::string data(buf.data(), buf.length());
    if (ignore_case) {
        std::transform(data.begin(), data.end(), data.begin(), ::tolower);
    }
    if (data == "true") {
        result = true;
        return true;
    }
    if (data == "false") {
        result = false;
        return true;
    }
    return false;
}

inline bool buf2double(string_view buf, double &result)
{
    if (buf.empty()) {
        return false;
    }

    std::string str(buf.data(), buf.length());
    const int saved_errno = errno;
    errno = 0;
    char *p = nullptr;
    double v = std::strtod(str.data(), &p);

    if (p - str.data() != str.length()) {
        return false;
    }

    if (v == HUGE_VAL || v == -HUGE_VAL || std::isnan(v) || errno != 0) {
        return false;
    }

    if (errno == 0) {
        errno = saved_errno;
    }

    result = v;
    return true;
}
} // namespace dsn

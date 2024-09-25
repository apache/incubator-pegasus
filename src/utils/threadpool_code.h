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

#include <ostream>

#include "ports.h"
#include "utils/fmt_utils.h"

namespace dsn {
class threadpool_code
{
public:
    threadpool_code() { _internal_code = 0; }
    explicit threadpool_code(int c) : _internal_code(c) {}
    threadpool_code(const threadpool_code &r) { _internal_code = r._internal_code; }
    explicit threadpool_code(const char *name);
    const char *to_string() const;
    threadpool_code &operator=(const threadpool_code &source)
    {
        _internal_code = source._internal_code;
        return *this;
    }
    bool operator==(const threadpool_code &r) { return _internal_code == r._internal_code; }
    bool operator!=(const threadpool_code &r) { return !(*this == r); }
    operator int() const { return _internal_code; }

    static int max();
    static bool is_exist(const char *name);

    friend std::ostream &operator<<(std::ostream &os, const threadpool_code &pool_code)
    {
        return os << pool_code.to_string();
    }

private:
    int _internal_code;
};

/*! define a new thread pool named x*/
#define DEFINE_THREAD_POOL_CODE(x) __selectany const ::dsn::threadpool_code x(#x);

DEFINE_THREAD_POOL_CODE(THREAD_POOL_INVALID)
DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEFAULT)
} // namespace dsn

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::threadpool_code);

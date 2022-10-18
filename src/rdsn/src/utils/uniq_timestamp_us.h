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

#include <cstdint>
#include <algorithm>
#include "utils/ports.h"
#include "runtime/api_layer1.h"

namespace dsn {
//
// uniq_timestamp_us is used to generate an increasing unique microsecond timestamp
// in rdsn, it's mainly used for replica to set mutation's timestamp
//
// Notice: this module is not thread-safe,
// please ensure that it is accessed only by one thread
//
class uniq_timestamp_us
{
private:
    uint64_t _last_ts;

public:
    uniq_timestamp_us() { _last_ts = dsn_now_us(); }

    void try_update(uint64_t new_ts)
    {
        if (dsn_likely(new_ts > _last_ts))
            _last_ts = new_ts;
    }

    uint64_t next()
    {
        _last_ts = std::max(dsn_now_us(), _last_ts + 1);
        return _last_ts;
    }
};
}

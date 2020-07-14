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

#include <stdint.h>
#include <string>

namespace dsn {

namespace replication {

// Used for replica throttling.
// Different throttling strategies may use different 'request_units', which is
// the cost of each request. For QPS-based throttling, request_units=1.
// For size-based throttling, request_units is the bytes size of the incoming
// request.
//
// not thread safe
class throttling_controller
{
public:
    enum throttling_type
    {
        PASS,
        DELAY,
        REJECT
    };

public:
    throttling_controller();

    // Configures throttling strategy dynamically from app-envs.
    // The result of `delay_units` and `reject_units` are ensured greater than 0.
    // If user-given parameter is 0*delay*100, then delay_units=1, likewise for reject_units.
    //
    // return true if parse succeed.
    // return false if parse failed for the reason of invalid env_value.
    // if return false, the original value will not be changed.
    // 'parse_error' is set when return false.
    // 'changed' is set when return true.
    // 'old_env_value' is set when 'changed' is set to true.
    bool parse_from_env(const std::string &env_value,
                        int partition_count,
                        /*out*/ std::string &parse_error,
                        /*out*/ bool &changed,
                        /*out*/ std::string &old_env_value);

    // reset to no throttling.
    void reset(/*out*/ bool &changed, /*out*/ std::string &old_env_value);

    // if throttling is enabled.
    bool enabled() const { return _enabled; }

    // return the current env value.
    const std::string &env_value() const { return _env_value; }

    // do throttling control, return throttling type.
    // 'delay_ms' is set when the return type is not PASS.
    throttling_type
    control(const int64_t client_timeout_ms, int32_t request_units, /*out*/ int64_t &delay_ms);

private:
    friend class throttling_controller_test;

    bool _enabled;
    std::string _env_value;
    int32_t _partition_count;
    int64_t _delay_units;     // should >= 0
    int64_t _delay_ms;        // should >= 0
    int64_t _reject_units;    // should >= 0
    int64_t _reject_delay_ms; // should >= 0
    int64_t _last_request_time;
    int64_t _cur_units;
};

} // namespace replication
} // namespace dsn

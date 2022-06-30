// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

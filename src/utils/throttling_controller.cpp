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

#include "throttling_controller.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <algorithm>
#include <memory>
#include <vector>

#include "runtime/api_layer1.h"
#include "utils/string_conv.h"
#include "utils/strings.h"

namespace dsn {
namespace replication {

throttling_controller::throttling_controller()
    : _enabled(false),
      _partition_count(0),
      _delay_units(0),
      _delay_ms(0),
      _reject_units(0),
      _reject_delay_ms(0),
      _last_request_time(0),
      _cur_units(0)
{
}

bool throttling_controller::parse_from_env(const std::string &env_value,
                                           int partition_count,
                                           std::string &parse_error,
                                           bool &changed,
                                           std::string &old_env_value)
{
    changed = false;
    if (_enabled && env_value == _env_value && partition_count == _partition_count)
        return true;
    std::vector<std::string> sargs;
    utils::split_args(env_value.c_str(), sargs, ',', true);
    if (sargs.empty()) {
        parse_error = "empty env value";
        return false;
    }
    bool delay_parsed = false;
    int64_t delay_units = 0;
    int64_t delay_ms = 0;
    bool reject_parsed = false;
    int64_t reject_units = 0;
    int64_t reject_delay_ms = 0;
    for (std::string &s : sargs) {
        std::vector<std::string> sargs1;
        utils::split_args(s.c_str(), sargs1, '*', true);
        if (sargs1.size() != 3) {
            parse_error = "invalid field count, should be 3";
            return false;
        }

        int64_t unit_multiplier = 1;
        if (!sargs1[0].empty()) {
            if (*sargs1[0].rbegin() == 'M') {
                unit_multiplier = 1000 * 1000;
            } else if (*sargs1[0].rbegin() == 'K') {
                unit_multiplier = 1000;
            }
            if (unit_multiplier != 1) {
                sargs1[0].pop_back();
            }
        }
        int64_t units = 0;
        if (!buf2int64(sargs1[0], units) || units < 0) {
            parse_error = "invalid units, should be non-negative int";
            return false;
        }
        units *= unit_multiplier;

        int64_t ms = 0;
        if (!buf2int64(sargs1[2], ms) || ms < 0) {
            parse_error = "invalid delay ms, should be non-negative int";
            return false;
        }
        if (sargs1[1] == "delay") {
            if (delay_parsed) {
                parse_error = "duplicate delay config";
                return false;
            }
            delay_parsed = true;
            delay_units = units / partition_count + 1;
            delay_ms = ms;
        } else if (sargs1[1] == "reject") {
            if (reject_parsed) {
                parse_error = "duplicate reject config";
                return false;
            }
            reject_parsed = true;
            reject_units = units / partition_count + 1;
            reject_delay_ms = ms;
        } else {
            parse_error = "invalid throttling type";
            return false;
        }
    }
    changed = true;
    old_env_value = _env_value;
    _enabled = true;
    _env_value = env_value;
    _partition_count = partition_count;
    _delay_units = delay_units;
    _delay_ms = delay_ms;
    _reject_units = reject_units;
    _reject_delay_ms = reject_delay_ms;
    return true;
}

void throttling_controller::reset(bool &changed, std::string &old_env_value)
{
    if (_enabled) {
        changed = true;
        old_env_value = _env_value;
        _enabled = false;
        _env_value.clear();
        _partition_count = 0;
        _delay_units = 0;
        _delay_ms = 0;
        _reject_units = 0;
        _reject_delay_ms = 0;
        _last_request_time = 0;
        _cur_units = 0;
    } else {
        changed = false;
    }
}

throttling_controller::throttling_type throttling_controller::control(
    const int64_t client_timeout_ms, int32_t request_units, int64_t &delay_ms)
{
    // return PASS if throttling controller is not enabled
    if (!_enabled) {
        return PASS;
    }

    int64_t now_s = dsn_now_s();
    if (now_s != _last_request_time) {
        _cur_units = 0;
        _last_request_time = now_s;
    }
    _cur_units += request_units;
    if (_reject_units > 0 && _cur_units > _reject_units) {
        _cur_units -= request_units;
        if (client_timeout_ms > 0) {
            delay_ms = std::min(_reject_delay_ms, client_timeout_ms / 2);
        } else {
            delay_ms = _reject_delay_ms;
        }
        return REJECT;
    }
    if (_delay_units > 0 && _cur_units > _delay_units) {
        if (client_timeout_ms > 0) {
            delay_ms = std::min(_delay_ms, client_timeout_ms / 2);
        } else {
            delay_ms = _delay_ms;
        }
        return DELAY;
    }
    return PASS;
}

} // namespace replication
} // namespace dsn

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

#include <fmt/core.h>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <algorithm>
#include <memory>
#include <vector>

#include "runtime/api_layer1.h"
#include "utils/string_conv.h"
#include "utils/strings.h"

namespace dsn {
namespace utils {

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

bool throttling_controller::parse_unit(std::string arg,
                                       /*out*/ uint64_t &units,
                                       /*out*/ std::string &hint_message)
{
    hint_message.clear();
    // Extract multiplier.
    uint64_t unit_multiplier = 1;
    if (!arg.empty()) {
        switch (*arg.rbegin()) {
        case 'M':
            unit_multiplier = 1 << 20;
            break;
        case 'K':
            unit_multiplier = 1 << 10;
            break;
        default:
            // Maybe a number, it's valid.
            break;
        }
        // Remove the tail 'M' or 'K'.
        if (unit_multiplier != 1) {
            arg.pop_back();
        }
    }

    // Parse value.
    uint64_t tmp;
    if (!buf2uint64(arg, tmp)) {
        hint_message = fmt::format("'{}' should be an unsigned integer", arg);
        return false;
    }

    // Check overflow.
    uint64_t result;
    if (__builtin_mul_overflow(tmp, unit_multiplier, &result)) {
        hint_message = fmt::format("'{}' result is overflow", arg);
        return false;
    }
    units = tmp * unit_multiplier;

    return true;
}

throttling_controller::ParseResult
throttling_controller::parse_from_env(const std::string &arg,
                                      const std::string &type,
                                      /*out*/ uint64_t &units,
                                      /*out*/ uint64_t &delay_ms,
                                      /*out*/ std::string &hint_message)
{
    hint_message.clear();
    std::vector<std::string> sub_args;
    utils::split_args(arg.c_str(), sub_args, '*', true);
    if (sub_args.size() != 3) {
        hint_message = fmt::format("The field count of '{}' separated by '*' must be 3", arg);
        return ParseResult::kFail;
    }

    // 1. Check the first part, which must be a positive number, optionally followed with 'K' or
    // 'M'.
    uint64_t u = 0;
    if (!parse_unit(sub_args[0], u, hint_message)) {
        return ParseResult::kFail;
    }

    // 2. Check the second part, which must be "delay" or "reject"
    if (sub_args[1] != type) {
        hint_message = fmt::format("'{}' should be 'delay' or 'reject'", sub_args[1]);
        return ParseResult::kIgnore;
    }

    // 3. Check the third part, which must be an unsigned integer.
    uint64_t ms = 0;
    if (!buf2uint64(sub_args[2], ms)) {
        hint_message = fmt::format("'{}' should be an unsigned integer", sub_args[2]);
        return ParseResult::kFail;
    }

    units = u;
    delay_ms = ms;
    return ParseResult::kSuccess;
}

bool throttling_controller::parse_from_env(const std::string &env_value,
                                           /*out*/ uint64_t &delay_units,
                                           /*out*/ uint64_t &delay_ms,
                                           /*out*/ uint64_t &reject_units,
                                           /*out*/ uint64_t &reject_delay_ms,
                                           /*out*/ std::string &hint_message)
{
    hint_message.clear();
    std::vector<std::string> sargs;
    utils::split_args(env_value.c_str(), sargs, ',');
    if (sargs.empty()) {
        hint_message = "The value shouldn't be empty";
        return false;
    }

    // Example for sarg: 100K*delay*100, 100M*reject*100, etc.
    bool delay_parsed = false;
    bool reject_parsed = false;
    for (const auto &sarg : sargs) {
        uint64_t units = 0;
        uint64_t ms = 0;
        // Check the "delay" args.
        auto result = parse_from_env(sarg, "delay", units, ms, hint_message);
        if (result == ParseResult::kFail) {
            return false;
        }

        if (result == ParseResult::kSuccess) {
            if (delay_parsed) {
                hint_message = "duplicate 'delay' config";
                return false;
            }
            delay_parsed = true;
            delay_units = units;
            delay_ms = ms;
            continue;
        }

        // Check the "reject" args.
        result = parse_from_env(sarg, "reject", units, ms, hint_message);
        if (result == ParseResult::kSuccess) {
            if (reject_parsed) {
                hint_message = "duplicate 'reject' config";
                return false;
            }
            reject_parsed = true;
            reject_units = units;
            reject_delay_ms = ms;
            continue;
        }

        if (hint_message.empty()) {
            hint_message = fmt::format("only 'delay' or 'reject' is supported", sarg);
        }
        return false;
    }
    return true;
}

bool throttling_controller::parse_from_env(const std::string &env_value,
                                           int partition_count,
                                           std::string &hint_message,
                                           bool &changed,
                                           std::string &old_env_value)
{
    hint_message.clear();
    changed = false;
    if (_enabled && env_value == _env_value && partition_count == _partition_count) {
        return true;
    }

    uint64_t delay_units = 0;
    uint64_t delay_ms = 0;
    uint64_t reject_units = 0;
    uint64_t reject_delay_ms = 0;
    if (!parse_from_env(
            env_value, delay_units, delay_ms, reject_units, reject_delay_ms, hint_message)) {
        return false;
    }

    changed = true;
    old_env_value = _env_value;
    _enabled = true;
    _env_value = env_value;
    _partition_count = partition_count;
    _delay_units = delay_units == 0 ? 0 : (delay_units / partition_count + 1);
    _delay_ms = delay_ms;
    _reject_units = reject_units == 0 ? 0 : (reject_units / partition_count + 1);
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

} // namespace utils
} // namespace dsn

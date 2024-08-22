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

#include "token_bucket_throttling_controller.h"

#include <boost/optional/optional.hpp>
#include <algorithm>

#include "string_conv.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/throttling_controller.h"

namespace dsn {
namespace utils {

token_bucket_throttling_controller::token_bucket_throttling_controller()
    : _enabled(false), _partition_count(0), _rate(0), _burstsize(0)
{
    _token_bucket = std::make_unique<DynamicTokenBucket>();
}

bool token_bucket_throttling_controller::consume_token(int32_t request_units)
{
    if (!_enabled) {
        return true;
    }
    auto res =
        _token_bucket->consumeWithBorrowNonBlocking((double)request_units, _rate, _burstsize);

    return (res.get_value_or(0) == 0);
}

bool token_bucket_throttling_controller::available() const
{
    if (!_enabled) {
        return true;
    }

    return _token_bucket->available(_rate, _burstsize) > 0;
}

void token_bucket_throttling_controller::reset(bool &changed, std::string &old_env_value)
{
    if (_enabled) {
        changed = true;
        old_env_value = _env_value;
        _enabled = false;
        _env_value.clear();
        _partition_count = 0;
        _rate = 0;
        _burstsize = 0;
    } else {
        changed = false;
    }
}

// return the current env value.
const std::string &token_bucket_throttling_controller::env_value() const { return _env_value; }

bool token_bucket_throttling_controller::parse_from_env(const std::string &env_value,
                                                        int32_t partition_count,
                                                        std::string &parse_error,
                                                        bool &changed,
                                                        std::string &old_env_value)
{
    old_env_value = _env_value;
    changed = false;

    if (_enabled && dsn_likely(env_value == _env_value) &&
        dsn_likely(partition_count == _partition_count)) {
        return true;
    }

    uint64_t reject_size_value;
    bool enabled;
    if (!transform_env_string(env_value, reject_size_value, enabled, parse_error)) {
        return false;
    }

    changed = true;

    _enabled = enabled;
    _env_value = env_value;
    _partition_count = partition_count;
    _rate = reject_size_value / std::max(partition_count, 1);
    _burstsize = _rate;
    return true;
}

bool token_bucket_throttling_controller::validate(const std::string &env, std::string &hint_message)
{
    uint64_t temp;
    bool temp_bool;
    return transform_env_string(env, temp, temp_bool, hint_message);
};

bool token_bucket_throttling_controller::transform_env_string(const std::string &env,
                                                              uint64_t &reject_size_value,
                                                              bool &enabled,
                                                              std::string &hint_message)
{
    hint_message.clear();
    enabled = true;

    // format like "200"
    if (buf2uint64(env, reject_size_value) && reject_size_value > 0) {
        return true;
    }

    // format like "200K"
    if (throttling_controller::parse_unit(env, reject_size_value, hint_message) &&
        reject_size_value > 0) {
        return true;
    }

    // format like "20000*delay*100", it's not supported.
    {
        uint64_t units = 0;
        uint64_t ms = 0;
        if (throttling_controller::parse_from_env(env, "delay", units, ms, hint_message) ==
            throttling_controller::ParseResult::kSuccess) {
            // rate must > 0 in TokenBucket.h
            reject_size_value = 1;
            enabled = false;

            LOG_DEBUG(
                "token_bucket_throttling_controller doesn't support delay method, so throttling "
                "controller is disabled now");
            return true;
        }
    }

    // format like "20000*delay*100,20000*reject*100"
    uint64_t reject_units = 0;
    {
        uint64_t delay_units = 0;
        uint64_t delay_ms = 0;
        uint64_t reject_delay_ms = 0;
        if (!throttling_controller::parse_from_env(
                env, delay_units, delay_ms, reject_units, reject_delay_ms, hint_message)) {
            return false;
        }

        if (reject_units == 0) {
            hint_message = "reject value should be greater than 0";
            return false;
        }
    }
    reject_size_value = reject_units;
    return true;
}

} // namespace utils
} // namespace dsn

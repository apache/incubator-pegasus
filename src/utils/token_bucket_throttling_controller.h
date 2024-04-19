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
#include <chrono>
#include <memory>
#include <string>

#include "utils/TokenBucket.h"

namespace dsn {
namespace utils {

using DynamicTokenBucket = folly::BasicDynamicTokenBucket<std::chrono::steady_clock>;

// token_bucket_throttling_controller ignores `delay` parameter
class token_bucket_throttling_controller
{
private:
    friend class token_bucket_throttling_controller_test;

    std::unique_ptr<DynamicTokenBucket> _token_bucket;

    bool _enabled;
    std::string _env_value;
    int32_t _partition_count = 0;
    double _rate;
    double _burstsize;

public:
    token_bucket_throttling_controller();

    // return ture means you can get token
    // return false means the bucket is already empty, but the token is borrowed from future.
    // non-blocking
    bool consume_token(int32_t request_units);

    // if the bucket has no tokens, return false
    bool available() const;

    // reset to no throttling.
    void reset(bool &changed, std::string &old_env_value);

    // return the current env value.
    const std::string &env_value() const;

    // Configures throttling strategy dynamically from app-envs.
    //
    // Support two style format:
    // 1. style: "20000*delay*100,20000*reject*100"
    //      example: 20000*delay*100,20000*reject*100
    //      result: reject 20000 request_units, but never delay
    //      example: 20000*delay*100
    //      result: never reject or delay
    //      example: 20000*reject*100
    //      result: reject 20000 request_units
    // 2. style: 20/"20K"/"20M"
    //      example: 20K
    //      result: reject 20000 request_units
    //
    // return true if parse succeed.
    // return false if parse failed for the reason of invalid env_value.
    // if return false, the original value will not be changed.
    // 'parse_error' is set when return false.
    // 'changed' is set when return true.
    // 'old_env_value' is set when 'changed' is set to true.
    bool parse_from_env(const std::string &env_value,
                        int32_t partition_count,
                        std::string &parse_error,
                        bool &changed,
                        std::string &old_env_value);

    // wrapper of transform_env_string, check if the env string is validated.
    static bool validate(const std::string &env, std::string &hint_message);

    static bool transform_env_string(const std::string &env,
                                     uint64_t &reject_size_value,
                                     bool &enabled,
                                     std::string &hint_message);
};

} // namespace utils
} // namespace dsn

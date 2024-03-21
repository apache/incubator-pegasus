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

#include "app_env_validator.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <stdint.h>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/replica_envs.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/token_bucket_throttling_controller.h"

namespace dsn {
namespace replication {
bool validate_app_envs(const std::map<std::string, std::string> &envs)
{
    // only check rocksdb app envs currently

    for (const auto &it : envs) {
        if (replica_envs::ROCKSDB_STATIC_OPTIONS.find(it.first) ==
                replica_envs::ROCKSDB_STATIC_OPTIONS.end() &&
            replica_envs::ROCKSDB_DYNAMIC_OPTIONS.find(it.first) ==
                replica_envs::ROCKSDB_DYNAMIC_OPTIONS.end()) {
            continue;
        }
        std::string hint_message;
        if (!validate_app_env(it.first, it.second, hint_message)) {
            LOG_WARNING(
                "app env {}={} is invaild, hint_message:{}", it.first, it.second, hint_message);
            return false;
        }
    }
    return true;
}

bool validate_app_env(const std::string &env_name,
                      const std::string &env_value,
                      std::string &hint_message)
{
    return app_env_validator::instance().validate_app_env(env_name, env_value, hint_message);
}

bool check_slow_query(const std::string &env_value, std::string &hint_message)
{
    uint64_t threshold = 0;
    if (!dsn::buf2uint64(env_value, threshold) ||
        threshold < replica_envs::MIN_SLOW_QUERY_THRESHOLD_MS) {
        hint_message = fmt::format("Slow query threshold must be >= {}ms",
                                   replica_envs::MIN_SLOW_QUERY_THRESHOLD_MS);
        return false;
    }
    return true;
}

bool check_deny_client(const std::string &env_value, std::string &hint_message)
{
    std::vector<std::string> sub_sargs;
    utils::split_args(env_value.c_str(), sub_sargs, '*', true);

    std::string invalid_hint_message = "Invalid deny client args, valid include: timeout*all, "
                                       "timeout*write, timeout*read; reconfig*all, reconfig*write, "
                                       "reconfig*read";
    if (sub_sargs.size() != 2) {
        hint_message = invalid_hint_message;
        return false;
    }
    if ((sub_sargs[0] != "timeout" && sub_sargs[0] != "reconfig") ||
        (sub_sargs[1] != "all" && sub_sargs[1] != "write" && sub_sargs[1] != "read")) {
        hint_message = invalid_hint_message;
        return false;
    }
    return true;
}

bool check_rocksdb_iteration(const std::string &env_value, std::string &hint_message)
{
    uint64_t threshold = 0;
    if (!dsn::buf2uint64(env_value, threshold) || threshold < 0) {
        hint_message = "Rocksdb iteration threshold must be greater than zero";
        return false;
    }
    return true;
}

bool check_throttling(const std::string &env_value, std::string &hint_message)
{
    std::vector<std::string> sargs;
    utils::split_args(env_value.c_str(), sargs, ',');
    if (sargs.empty()) {
        hint_message = "The value shouldn't be empty";
        return false;
    }

    // example for sarg: 100K*delay*100 / 100M*reject*100
    bool reject_parsed = false;
    bool delay_parsed = false;
    for (std::string &sarg : sargs) {
        std::vector<std::string> sub_sargs;
        utils::split_args(sarg.c_str(), sub_sargs, '*', true);
        if (sub_sargs.size() != 3) {
            hint_message = fmt::format("The field count of {} should be 3", sarg);
            return false;
        }

        // check the first part, which is must be a positive number followed with 'K' or 'M'
        int64_t units = 0;
        if (!sub_sargs[0].empty() &&
            ('M' == *sub_sargs[0].rbegin() || 'K' == *sub_sargs[0].rbegin())) {
            sub_sargs[0].pop_back();
        }
        if (!buf2int64(sub_sargs[0], units) || units < 0) {
            hint_message = fmt::format("{} should be non-negative int", sub_sargs[0]);
            return false;
        }

        // check the second part, which is must be "delay" or "reject"
        if (sub_sargs[1] == "delay") {
            if (delay_parsed) {
                hint_message = "duplicate delay config";
                return false;
            }
            delay_parsed = true;
        } else if (sub_sargs[1] == "reject") {
            if (reject_parsed) {
                hint_message = "duplicate reject config";
                return false;
            }
            reject_parsed = true;
        } else {
            hint_message = fmt::format("{} should be \"delay\" or \"reject\"", sub_sargs[1]);
            return false;
        }

        // check the third part, which is must be a positive number or 0
        int64_t delay_ms = 0;
        if (!buf2int64(sub_sargs[2], delay_ms) || delay_ms < 0) {
            hint_message = fmt::format("{} should be non-negative int", sub_sargs[2]);
            return false;
        }
    }

    return true;
}

bool check_bool_value(const std::string &env_value, std::string &hint_message)
{
    bool result = false;
    if (!dsn::buf2bool(env_value, result)) {
        hint_message = fmt::format("invalid string {}, should be \"true\" or \"false\"", env_value);
        return false;
    }
    return true;
}

bool check_rocksdb_write_buffer_size(const std::string &env_value, std::string &hint_message)
{
    uint64_t val = 0;

    if (!dsn::buf2uint64(env_value, val)) {
        hint_message = fmt::format("rocksdb.write_buffer_size cannot set this val: {}", env_value);
        return false;
    }
    if (val < (16 << 20) || val > (512 << 20)) {
        hint_message =
            fmt::format("rocksdb.write_buffer_size suggest set val in range [16777216, 536870912]");
        return false;
    }
    return true;
}
bool check_rocksdb_num_levels(const std::string &env_value, std::string &hint_message)
{
    int32_t val = 0;

    if (!dsn::buf2int32(env_value, val)) {
        hint_message = fmt::format("rocksdb.num_levels cannot set this val: {}", env_value);
        return false;
    }
    if (val < 1 || val > 10) {
        hint_message = fmt::format("rocksdb.num_levels suggest set val in range [1 , 10]");
        return false;
    }
    return true;
}

bool app_env_validator::validate_app_env(const std::string &env_name,
                                         const std::string &env_value,
                                         std::string &hint_message)
{
    auto func_iter = _validator_funcs.find(env_name);
    if (func_iter != _validator_funcs.end()) {
        // check function == nullptr means no check
        if (nullptr != func_iter->second && !func_iter->second(env_value, hint_message)) {
            LOG_WARNING("{}={} is invalid.", env_name, env_value);
            return false;
        }

        return true;
    }

    hint_message = fmt::format("app_env \"{}\" is not supported", env_name);
    return false;
}

void app_env_validator::register_all_validators()
{
    _validator_funcs = {
        {replica_envs::SLOW_QUERY_THRESHOLD,
         std::bind(&check_slow_query, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::WRITE_QPS_THROTTLING,
         std::bind(&check_throttling, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::WRITE_SIZE_THROTTLING,
         std::bind(&check_throttling, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::ROCKSDB_ITERATION_THRESHOLD_TIME_MS,
         std::bind(&check_rocksdb_iteration, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::ROCKSDB_BLOCK_CACHE_ENABLED,
         std::bind(&check_bool_value, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::READ_QPS_THROTTLING,
         std::bind(&check_throttling, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::READ_SIZE_THROTTLING,
         std::bind(&utils::token_bucket_throttling_controller::validate,
                   std::placeholders::_1,
                   std::placeholders::_2)},
        {replica_envs::SPLIT_VALIDATE_PARTITION_HASH,
         std::bind(&check_bool_value, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::USER_SPECIFIED_COMPACTION, nullptr},
        {replica_envs::BACKUP_REQUEST_QPS_THROTTLING,
         std::bind(&check_throttling, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND,
         std::bind(&check_bool_value, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::DENY_CLIENT_REQUEST,
         std::bind(&check_deny_client, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
         std::bind(&check_rocksdb_write_buffer_size, std::placeholders::_1, std::placeholders::_2)},
        {replica_envs::ROCKSDB_NUM_LEVELS,
         std::bind(&check_rocksdb_num_levels, std::placeholders::_1, std::placeholders::_2)},
        // TODO(zhaoliwei): not implemented
        {replica_envs::BUSINESS_INFO, nullptr},
        {replica_envs::TABLE_LEVEL_DEFAULT_TTL, nullptr},
        {replica_envs::ROCKSDB_USAGE_SCENARIO, nullptr},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT, nullptr},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS, nullptr},
        {replica_envs::MANUAL_COMPACT_DISABLED, nullptr},
        {replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT, nullptr},
        {replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME, nullptr},
        {replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL, nullptr},
        {replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION, nullptr},
        {replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME, nullptr},
        {replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL, nullptr},
        {replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION, nullptr},
        {replica_envs::REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS, nullptr},
        {replica_envs::REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES, nullptr},
    };
}

} // namespace replication
} // namespace dsn

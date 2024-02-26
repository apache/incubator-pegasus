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
#include <nlohmann/json.hpp>
#include <stdint.h>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/replica_envs.h"
#include "http/http_status_code.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/token_bucket_throttling_controller.h"

namespace dsn {
namespace replication {
bool app_env_validator::validate_app_envs(const std::map<std::string, std::string> &envs)
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
                "app env {}={} is invalid, hint_message:{}", it.first, it.second, hint_message);
            return false;
        }
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
            hint_message = fmt::format("The field count of '{}' separated by '*' must be 3", sarg);
            return false;
        }

        // check the first part, which must be a positive number followed with 'K' or 'M'
        uint64_t units = 0;
        if (!sub_sargs[0].empty() &&
            ('M' == *sub_sargs[0].rbegin() || 'K' == *sub_sargs[0].rbegin())) {
            sub_sargs[0].pop_back();
        }
        if (!buf2uint64(sub_sargs[0], units)) {
            hint_message = fmt::format("'{}' should be an unsigned integer", sub_sargs[0]);
            return false;
        }

        // check the second part, which must be "delay" or "reject"
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
            hint_message = fmt::format("'{}' should be 'delay' or 'reject'", sub_sargs[1]);
            return false;
        }

        // check the third part, which must be a positive number or 0
        uint64_t delay_ms = 0;
        if (!buf2uint64(sub_sargs[2], delay_ms)) {
            hint_message = fmt::format("'{}' should be an unsigned integer", sub_sargs[2]);
            return false;
        }
    }

    return true;
}

void app_env_validator::EnvInfo::init()
{
    // Set default limitation description.
    if (limit_desc.empty()) {
        switch (type) {
        case ValueType::kBool:
            limit_desc = "true | false";
            break;
        case ValueType::kString:
            break;
        default:
            CHECK_TRUE(false);
            __builtin_unreachable();
        }
    }

    // Set default sample.
    if (sample.empty()) {
        switch (type) {
        case ValueType::kBool:
            sample = "true";
            break;
        case ValueType::kString:
            break;
        default:
            CHECK_TRUE(false);
            __builtin_unreachable();
        }
    }
}

app_env_validator::EnvInfo::EnvInfo(ValueType t) : type(t)
{
    CHECK_TRUE(type == ValueType::kBool);
    init();
}

app_env_validator::EnvInfo::EnvInfo(ValueType t,
                                    std::string ld,
                                    std::string s,
                                    string_validator_func v)
    : type(t), limit_desc(std::move(ld)), sample(std::move(s)), string_validator(std::move(v))
{
    CHECK_TRUE(type == ValueType::kString);
    init();
}

app_env_validator::EnvInfo::EnvInfo(ValueType t,
                                    std::string ld,
                                    std::string s,
                                    int_validator_func v)
    : type(t), limit_desc(std::move(ld)), sample(std::move(s)), int_validator(std::move(v))
{
    CHECK_TRUE(type == ValueType::kInt64);
    init();
}

bool app_env_validator::validate_app_env(const std::string &env_name,
                                         const std::string &env_value,
                                         std::string &hint_message)
{
    // Check if the env is supported.
    const auto func_iter = _validator_funcs.find(env_name);
    if (func_iter == _validator_funcs.end()) {
        hint_message = fmt::format("app_env \"{}\" is not supported", env_name);
        return false;
    }

    switch (func_iter->second.type) {
    case ValueType::kBool: {
        // Check by the default boolean validator.
        bool result = false;
        if (!dsn::buf2bool(env_value, result)) {
            hint_message = fmt::format("invalid value '{}', should be a boolean", env_value);
            return false;
        }
        break;
    }
    case ValueType::kInt64: {
        // Check by the default int64 validator.
        int64_t result = 0;
        if (!dsn::buf2int64(env_value, result)) {
            hint_message = fmt::format("invalid value '{}', should be an integer", env_value);
            return false;
        }

        // Check by the self defined validator.
        if (nullptr != func_iter->second.int_validator &&
            !func_iter->second.int_validator(result)) {
            hint_message = fmt::format(
                "invalid value '{}', should be {}", env_value, func_iter->second.limit_desc);
            return false;
        }
        break;
    }
    case ValueType::kString: {
        // Check by the self defined validator.
        if (nullptr != func_iter->second.string_validator &&
            !func_iter->second.string_validator(env_value, hint_message)) {
            return false;
        }
        break;
    }
    default:
        CHECK_TRUE(false);
        __builtin_unreachable();
    }

    return true;
}

void app_env_validator::register_all_validators()
{
    static const auto kMinWriteBufferSize = 16 << 20;
    static const auto kMaxWriteBufferSize = 512 << 20;
    static const auto kMinLevel = 1;
    static const auto kMaxLevel = 10;
    static const std::string check_throttling_limit = "<size[K|M]>*<delay|reject>*<milliseconds>";
    static const std::string check_throttling_sample = "10000*delay*100,20000*reject*100";
    _validator_funcs = {
        {replica_envs::SLOW_QUERY_THRESHOLD,
         {ValueType::kInt64,
          fmt::format(">= {}", replica_envs::MIN_SLOW_QUERY_THRESHOLD_MS),
          "1000",
          [](int64_t new_value) {
              return replica_envs::MIN_SLOW_QUERY_THRESHOLD_MS <= new_value;
          }}},
        {replica_envs::WRITE_QPS_THROTTLING,
         {ValueType::kString, check_throttling_limit, check_throttling_sample, &check_throttling}},
        {replica_envs::WRITE_SIZE_THROTTLING,
         {ValueType::kString, check_throttling_limit, check_throttling_sample, &check_throttling}},
        {replica_envs::ROCKSDB_ITERATION_THRESHOLD_TIME_MS,
         {ValueType::kInt64, ">= 0", "1000", [](int64_t new_value) { return new_value >= 0; }}},
        {replica_envs::ROCKSDB_BLOCK_CACHE_ENABLED, {ValueType::kBool}},
        {replica_envs::READ_QPS_THROTTLING,
         {ValueType::kString, check_throttling_limit, check_throttling_sample, &check_throttling}},
        {replica_envs::READ_SIZE_THROTTLING,
         {ValueType::kString,
          "",
          "20000*delay*100,20000*reject*100",
          &utils::token_bucket_throttling_controller::validate}},
        {replica_envs::SPLIT_VALIDATE_PARTITION_HASH, {ValueType::kBool}},
        {replica_envs::USER_SPECIFIED_COMPACTION, {ValueType::kString}},
        {replica_envs::BACKUP_REQUEST_QPS_THROTTLING,
         {ValueType::kString, check_throttling_limit, check_throttling_sample, &check_throttling}},
        {replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND, {ValueType::kBool}},
        {replica_envs::DENY_CLIENT_REQUEST,
         {ValueType::kString,
          "timeout*all | timeout*write | timeout*read | reconfig*all | reconfig*write | "
          "reconfig*read",
          "timeout*all",
          &check_deny_client}},
        {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
         {ValueType::kInt64,
          fmt::format("In range [{}, {}]", kMinWriteBufferSize, kMaxWriteBufferSize),
          fmt::format("{}", kMinWriteBufferSize),
          [](int64_t new_value) {
              return kMinWriteBufferSize <= new_value && new_value <= kMaxWriteBufferSize;
          }}},
        {replica_envs::ROCKSDB_NUM_LEVELS,
         {ValueType::kInt64,
          fmt::format("In range [{}, {}]", kMinLevel, kMaxLevel),
          "6",
          [](int64_t new_value) { return kMinLevel <= new_value && new_value <= kMaxLevel; }}},
        // TODO(zhaoliwei): not implemented
        {replica_envs::BUSINESS_INFO, {ValueType::kString}},
        {replica_envs::TABLE_LEVEL_DEFAULT_TTL, {ValueType::kString}},
        {replica_envs::ROCKSDB_USAGE_SCENARIO, {ValueType::kString}},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT, {ValueType::kString}},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS, {ValueType::kString}},
        {replica_envs::MANUAL_COMPACT_DISABLED, {ValueType::kString}},
        {replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT, {ValueType::kString}},
        {replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME, {ValueType::kString}},
        {replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL,
         {ValueType::kInt64,
          "-1 or >= 1",
          "6",
          [](int64_t new_value) { return new_value == -1 || new_value >= 1; }}},
        {replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION, {ValueType::kString}},
        {replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME, {ValueType::kString}},
        {replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL,
         {ValueType::kInt64,
          "-1 or >= 1",
          "6",
          [](int64_t new_value) { return new_value == -1 || new_value >= 1; }}},
        {replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION, {ValueType::kString}},
        {replica_envs::REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS, {ValueType::kString}},
        {replica_envs::REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES, {ValueType::kString}}};
}

const std::unordered_map<app_env_validator::ValueType, std::string>
    app_env_validator::EnvInfo::ValueType2String{{ValueType::kBool, "bool"},
                                                 {ValueType::kInt64, "unsigned int"},
                                                 {ValueType::kString, "string"}};

nlohmann::json app_env_validator::EnvInfo::to_json() const
{
    const auto &type_str = ValueType2String.find(type);
    CHECK_TRUE(type_str != ValueType2String.end());
    nlohmann::json info;
    info["type"] = type_str->second;
    info["limitation"] = limit_desc;
    info["sample"] = sample;
    return info;
}

void app_env_validator::list_all_envs(const http_request &req, http_response &resp) const
{
    nlohmann::json envs;
    for (const auto &validator_func : _validator_funcs) {
        envs[validator_func.first] = validator_func.second.to_json();
    }
    resp.body = envs.dump(2);
    resp.status_code = http_status_code::kOk;
}

} // namespace replication
} // namespace dsn

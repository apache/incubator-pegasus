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
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <stdint.h>
#include <set>
#include <utility>
#include <vector>

#include "common/duplication_common.h"
#include "common/replica_envs.h"
#include "http/http_status_code.h"
#include "utils/fmt_logging.h"
#include "gutil/map_util.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/throttling_controller.h"
#include "utils/token_bucket_throttling_controller.h"

namespace dsn {
namespace replication {
app_env_validator::app_env_validator()
{
    register_all_validators();
    register_handler(
        "list",
        std::bind(
            &app_env_validator::list_all_envs, this, std::placeholders::_1, std::placeholders::_2),
        "List all available table environments.");
}

app_env_validator::~app_env_validator() { deregister_handler("list"); }

bool app_env_validator::validate_app_envs(const std::map<std::string, std::string> &envs)
{
    // only check rocksdb app envs currently
    for (const auto &[key, value] : envs) {
        if (!gutil::ContainsKey(replica_envs::ROCKSDB_STATIC_OPTIONS, key) &&
            !gutil::ContainsKey(replica_envs::ROCKSDB_DYNAMIC_OPTIONS, key)) {
            continue;
        }
        std::string hint_message;
        if (!validate_app_env(key, value, hint_message)) {
            LOG_ERROR("app env '{}={}' is invalid, hint_message: {}", key, value, hint_message);
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
    uint64_t delay_units = 0;
    uint64_t delay_ms = 0;
    uint64_t reject_units = 0;
    uint64_t reject_delay_ms = 0;
    return utils::throttling_controller::parse_from_env(
        env_value, delay_units, delay_ms, reject_units, reject_delay_ms, hint_message);
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

app_env_validator::EnvInfo::EnvInfo(ValueType t) : type(t) { init(); }

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
    CHECK_TRUE(type == ValueType::kInt64 || type == ValueType::kInt32);
    init();
}

bool app_env_validator::validate_app_env(const std::string &env_name,
                                         const std::string &env_value,
                                         std::string &hint_message)
{
    // Check if the env is supported.
    const auto *func = gutil::FindOrNull(_validator_funcs, env_name);
    if (func == nullptr) {
        hint_message = fmt::format("app_env '{}' is not supported", env_name);
        return false;
    }

    // 'int_result' will be used if the env variable is integer type.
    int64_t int_result = 0;
    switch (func->type) {
    case ValueType::kBool: {
        // Check by the default boolean validator.
        bool result = false;
        if (!dsn::buf2bool(env_value, result)) {
            hint_message = fmt::format("invalid value '{}', should be a boolean", env_value);
            return false;
        }
        break;
    }
    case ValueType::kInt32: {
        // Check by the default int32 validator.
        int32_t result = 0;
        if (!dsn::buf2int32(env_value, result)) {
            hint_message =
                fmt::format("invalid value '{}', should be an 32 bits integer", env_value);
            return false;
        }
        int_result = result;
        break;
    }
    case ValueType::kInt64: {
        // Check by the default int64 validator.
        int64_t result = 0;
        if (!dsn::buf2int64(env_value, result)) {
            hint_message =
                fmt::format("invalid value '{}', should be an 64 bits integer", env_value);
            return false;
        }
        int_result = result;
        break;
    }
    case ValueType::kString: {
        // Check by the self defined validator.
        if (nullptr != func->string_validator && !func->string_validator(env_value, hint_message)) {
            return false;
        }
        break;
    }
    default:
        CHECK_TRUE(false);
        __builtin_unreachable();
    }

    if (func->type == ValueType::kInt32 || func->type == ValueType::kInt64) {
        // Check by the self defined validator.
        if (nullptr != func->int_validator && !func->int_validator(int_result)) {
            hint_message =
                fmt::format("invalid value '{}', should be '{}'", env_value, func->limit_desc);
            return false;
        }
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

    // TODO(yingchun): Use a macro to simplify the following 2 code blocks.
    // EnvInfo for MANUAL_COMPACT_*_BOTTOMMOST_LEVEL_COMPACTION.
    const std::set<std::string> valid_mcblcs(
        {replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE,
         replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP});
    const std::string mcblc_sample(fmt::format("{}", fmt::join(valid_mcblcs, " | ")));
    const app_env_validator::EnvInfo mcblc(
        app_env_validator::ValueType::kString,
        mcblc_sample,
        replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP,
        [=](const std::string &new_value, std::string &hint_message) {
            if (valid_mcblcs.count(new_value) == 0) {
                hint_message = mcblc_sample;
                return false;
            }
            return true;
        });

    // EnvInfo for ROCKSDB_USAGE_SCENARIO.
    const std::set<std::string> valid_russ({replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_NORMAL,
                                            replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE,
                                            replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD});
    const std::string rus_sample(fmt::format("{}", fmt::join(valid_russ, " | ")));
    const app_env_validator::EnvInfo rus(
        app_env_validator::ValueType::kString,
        rus_sample,
        replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_NORMAL,
        [=](const std::string &new_value, std::string &hint_message) {
            if (valid_russ.count(new_value) == 0) {
                hint_message = rus_sample;
                return false;
            }
            return true;
        });

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
        {replica_envs::BUSINESS_INFO, {ValueType::kString}},
        {replica_envs::TABLE_LEVEL_DEFAULT_TTL,
         {ValueType::kInt32, ">= 0", "86400", [](int64_t new_value) { return new_value >= 0; }}},
        {replica_envs::ROCKSDB_USAGE_SCENARIO, rus},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT,
         {ValueType::kInt32, "> 0", "2", [](int64_t new_value) { return new_value > 0; }}},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS,
         {ValueType::kInt32, ">= 0", "3600", [](int64_t new_value) { return new_value >= 0; }}},
        {replica_envs::MANUAL_COMPACT_DISABLED, {ValueType::kBool}},
        {replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT,
         {ValueType::kInt32, ">= 0", "8", [](int64_t new_value) { return new_value >= 0; }}},
        {replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME,
         {ValueType::kInt64,
          "> 0, timestamp (in seconds) to trigger the once manual compaction",
          "1700000000",
          [](int64_t new_value) { return new_value >= 0; }}},
        {replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL,
         {ValueType::kInt64,
          "-1 or >= 1",
          "6",
          [](int64_t new_value) { return new_value == -1 || new_value >= 1; }}},
        {replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION, mcblc},
        // TODO(yingchun): enable the validator by refactoring
        // pegasus_manual_compact_service::check_periodic_compact
        {replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME, {ValueType::kString}},
        {replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL,
         {ValueType::kInt64,
          "-1 or >= 1",
          "6",
          [](int64_t new_value) { return new_value == -1 || new_value >= 1; }}},
        {replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION, mcblc},
        {replica_envs::REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS, {ValueType::kString}},
        {replica_envs::REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES, {ValueType::kString}},
        {duplication_constants::kEnvMasterClusterKey, {ValueType::kString}},
        {duplication_constants::kEnvMasterMetasKey, {ValueType::kString}},
        {duplication_constants::kEnvMasterAppNameKey, {ValueType::kString}},
        {duplication_constants::kEnvFollowerAppStatusKey, {ValueType::kString}},
    };
}

const std::unordered_map<app_env_validator::ValueType, std::string>
    app_env_validator::EnvInfo::ValueType2String{{ValueType::kBool, "bool"},
                                                 {ValueType::kInt32, "unsigned int32"},
                                                 {ValueType::kInt64, "unsigned int64"},
                                                 {ValueType::kString, "string"}};

nlohmann::json app_env_validator::EnvInfo::to_json() const
{
    const auto *type_str = gutil::FindOrNull(ValueType2String, type);
    CHECK_NOTNULL(type_str, "");
    nlohmann::json info;
    info["type"] = *type_str;
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

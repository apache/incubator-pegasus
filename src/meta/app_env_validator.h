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

#include <nlohmann/json.hpp> // IWYU pragma: keep
#include <nlohmann/json_fwd.hpp>
#include <stdint.h>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>

#include "http/http_server.h"

namespace dsn {
namespace replication {

class app_env_validator final : public http_service
{
public:
    app_env_validator();

    ~app_env_validator() final;

    std::string path() const override { return "envs"; }

    bool validate_app_env(const std::string &env_name,
                          const std::string &env_value,
                          std::string &hint_message);

    bool validate_app_envs(const std::map<std::string, std::string> &envs);

private:
    void register_all_validators();

    void list_all_envs(const http_request &req, http_response &resp) const;

public:
    // The type of table env.
    enum class ValueType : uint32_t
    {
        kBool,
        kInt32,
        kInt64,
        kString
    };

    // The type of table env and its limit description.
    struct EnvInfo
    {
        using string_validator_func = std::function<bool(const std::string &, std::string &)>;
        using int_validator_func = std::function<bool(int64_t)>;

        ValueType type;
        std::string limit_desc;
        std::string sample;
        string_validator_func string_validator;
        int_validator_func int_validator;

        // Construct an object.
        EnvInfo(ValueType t);

        // Construct an object with kString type.
        EnvInfo(ValueType t,
                std::string ld,
                std::string s,
                string_validator_func v = string_validator_func());

        // Construct an object with kInt64 type.
        EnvInfo(ValueType t,
                std::string ld,
                std::string s,
                int_validator_func v = int_validator_func());

        nlohmann::json to_json() const;

        static const std::unordered_map<app_env_validator::ValueType, std::string> ValueType2String;

    private:
        void init();
    };

    // The table envs and their limit descriptions, all available envs must be registered here.
    std::map<std::string, EnvInfo> _validator_funcs;
};

} // namespace replication
} // namespace dsn

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

#include <functional>
#include <map>
#include <string>
#include "utils/singleton.h"

namespace dsn {
namespace replication {

bool validate_app_envs(const std::map<std::string, std::string> &envs);

bool validate_app_env(const std::string &env_name,
                      const std::string &env_value,
                      std::string &hint_message);

class app_env_validator : public utils::singleton<app_env_validator>
{
public:
    bool validate_app_env(const std::string &env_name,
                          const std::string &env_value,
                          std::string &hint_message);

private:
    app_env_validator() { register_all_validators(); }
    ~app_env_validator() = default;

    void register_all_validators();

    using validator_func = std::function<bool(const std::string &, std::string &)>;
    std::map<std::string, validator_func> _validator_funcs;

    friend class utils::singleton<app_env_validator>;
};

} // namespace replication
} // namespace dsn

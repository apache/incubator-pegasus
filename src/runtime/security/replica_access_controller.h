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

#include <string>
#include <unordered_set>
#include <vector>

#include "access_controller.h"
#include "common/json_helper.h"
#include "runtime/ranger/access_type.h"
#include "runtime/ranger/ranger_resource_policy.h"
#include "utils/synchronize.h"

namespace dsn {
class message_ex;

namespace security {

using matched_database_table_policies = std::vector<ranger::matched_database_table_policy>;

class replica_access_controller : public access_controller
{
public:
    explicit replica_access_controller(const std::string &replica_name);

    // Check whether replica can be accessed, this method is compatible with ACL using
    // '_allowed_users' and ACL using Ranger policy.
    bool allowed(message_ex *msg, ranger::access_type req_type) const override;

    // Update '_allowed_users' when the app_env(REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS) of the
    // table changes
    void update_allowed_users(const std::string &users) override;

    // Update '_ranger_policies' when the app_env(REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES) of the
    // table changes
    void update_ranger_policies(const std::string &policies) override;

    DEFINE_JSON_SERIALIZATION(_ranger_policies);

private:
    // Security check to avoid allowed_users is not empty in special scenarios.
    void check_allowed_users_valid() const;

private:
    mutable utils::rw_lock_nr _lock;
    // Users will pass the access control in the old ACL.
    std::unordered_set<std::string> _allowed_users;

    // App_env(REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS) to facilitate whether to update
    // '_allowed_users'.
    std::string _env_users;

    // App_env(REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES) to facilitate whether to update
    // '_ranger_policies'.
    std::string _env_policies;

    // The Ranger policies for ACL.
    matched_database_table_policies _ranger_policies;

    std::string _name;

    friend class replica_access_controller_test;
};
} // namespace security
} // namespace dsn

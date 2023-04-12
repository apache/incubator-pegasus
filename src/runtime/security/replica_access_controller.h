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

#include "access_controller.h"
#include "runtime/ranger/access_type.h"
#include "runtime/ranger/ranger_resource_policy.h"
#include "utils/synchronize.h"

namespace dsn {
class message_ex;

namespace security {
class replica_access_controller : public access_controller
{
public:
    explicit replica_access_controller(const std::string &replica_name);
    bool allowed(message_ex *msg, ranger::access_type req_type) override;
    void update_allowed_users(const std::string &users) override;
    void start_to_dump_and_sync_policies(const std::string &policies) override;

private:
    utils::rw_lock_nr _lock; // [
    std::unordered_set<std::string> _allowed_users;
    std::string _env_users;
    // ]
    std::string _name;
    std::string _env_policies;
    ranger::acl_policies _ranger_policies;

    friend class replica_access_controller_test;
};
} // namespace security
} // namespace dsn

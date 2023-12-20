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

#include <memory>
#include <string>
#include <unordered_set>

#include "ranger/access_type.h"

namespace dsn {
class message_ex;

namespace ranger {
class ranger_resource_policy_manager;
}

namespace security {

class access_controller
{
public:
    access_controller();
    virtual ~access_controller();

    // Update the access controller.
    // users - the new allowed users to update
    virtual void update_allowed_users(const std::string &users) {}

    // Check whether the Ranger ACL is enabled or not.
    bool is_enable_ranger_acl() const;

    // Update the access controller policy
    // policies -  the new Ranger policies to update
    virtual void update_ranger_policies(const std::string &policies) {}

    // Check if the message received is allowd to access the system.
    // msg - the message received
    virtual bool allowed(message_ex *msg, dsn::ranger::access_type req_type) const { return false; }

    // Check if the message received is allowd to access the table.
    // msg - the message received
    // app_name - tables involved in ACL
    virtual bool allowed(message_ex *msg, const std::string &app_name = "") const { return false; }

protected:
    // Check if 'user_name' is the super user.
    bool is_super_user(const std::string &user_name) const;

    std::unordered_set<std::string> _super_users;

    friend class meta_access_controller_test;
};

std::shared_ptr<access_controller> create_meta_access_controller(
    const std::shared_ptr<ranger::ranger_resource_policy_manager> &policy_manager);

std::unique_ptr<access_controller>
create_replica_access_controller(const std::string &replica_name);
} // namespace security
} // namespace dsn

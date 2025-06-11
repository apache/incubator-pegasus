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
    virtual ~access_controller() = default;

    // Return true if Ranger ACL is enabled, otherwise false.
    static bool is_ranger_acl_enabled();

    // Return true if either Ranger ACL or old ACL is enabled, otherwise false.
    static bool is_acl_enabled();

    // Update allowed users for old ACL.
    //
    // Parameters:
    // - users: the new allowed users used to update.
    virtual void update_allowed_users(const std::string &users) {}

    // Update policies for Ranger ACL.
    //
    // Parameters:
    // - policies: the new policies used to update.
    virtual void update_ranger_policies(const std::string &policies) {}

    // Return true if the received request is allowd to access the system with specified
    // type, otherwise false.
    //
    // Parameters:
    // - msg: the received request, should never be null.
    // - req_type: the access type.
    virtual bool allowed(message_ex *msg, dsn::ranger::access_type req_type) const { return false; }

    // Return true if the received request is allowd to access the table, otherwise false.
    //
    // Parameters:
    // - msg: the received request, should never be null.
    // - app_name: the name of the table on which the ACL check is performed.
    virtual bool allowed(message_ex *msg, const std::string &app_name) const { return false; }

    // The same as the above function, except that `app_name` is set empty.
    bool allowed(message_ex *msg) const { return allowed(msg, ""); }

protected:
    // Check if 'user_name' is the super user.
    bool is_super_user(const std::string &user_name) const;

    std::unordered_set<std::string> _super_users;

    friend class SuperUserTest;
};

std::shared_ptr<access_controller> create_meta_access_controller(
    const std::shared_ptr<ranger::ranger_resource_policy_manager> &policy_manager);

std::unique_ptr<access_controller>
create_replica_access_controller(const std::string &replica_name);

} // namespace security
} // namespace dsn

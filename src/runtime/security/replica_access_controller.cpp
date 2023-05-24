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

#include <utility>

// Disable class-memaccess warning to facilitate compilation with gcc>7
// https://github.com/Tencent/rapidjson/issues/1700
#pragma GCC diagnostic push
#if defined(__GNUC__) && __GNUC__ >= 8
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
#include "common/json_helper.h"

#pragma GCC diagnostic pop

#include "replica_access_controller.h"
#include "runtime/rpc/network.h"
#include "runtime/rpc/rpc_message.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

namespace dsn {
namespace security {
DSN_DECLARE_bool(enable_acl);
DSN_DECLARE_bool(enable_ranger_acl);
replica_access_controller::replica_access_controller(const std::string &replica_name)
{
    _name = replica_name;
}

bool replica_access_controller::allowed(message_ex *msg, ranger::access_type req_type) const
{
    const std::string &user_name = msg->io_session->get_client_username();
    if (!FLAGS_enable_ranger_acl) {
        if (!FLAGS_enable_acl || is_super_user(user_name)) {
            return true;
        }
        {
            utils::auto_read_lock l(_lock);
            // If the user didn't specify any ACL, it means this table is publicly accessible to
            // everyone. This is a backdoor to allow old-version clients to gracefully upgrade.
            // After they are finally ensured to be fully upgraded, they can specify some usernames
            // to ACL and the table will be truly protected.
            if (!_allowed_users.empty() && _allowed_users.count(user_name) == 0) {
                LOG_INFO("{}: user_name({}) doesn't exist in acls map", _name, user_name);
                return false;
            }
            return true;
        }
    }

    // use Ranger policy for ACL.
    {
        utils::auto_read_lock l(_lock);
        return check_ranger_database_table_policy_allowed(_ranger_policies, req_type, user_name) ==
               ranger::access_control_result::kAllowed;
    }
}

void replica_access_controller::update_allowed_users(const std::string &users)
{
    {
        // check to see whether we should update it or not.
        utils::auto_read_lock l(_lock);
        if (_env_users == users) {
            check_allowed_users_valid();
            return;
        }
    }

    std::unordered_set<std::string> users_set;
    utils::split_args(users.c_str(), users_set, ',');
    {
        utils::auto_write_lock l(_lock);
        _allowed_users.swap(users_set);
        _env_users = users;
        check_allowed_users_valid();
    }
}

void replica_access_controller::update_ranger_policies(const std::string &policies)
{
    {
        utils::auto_read_lock l(_lock);
        if (_env_policies == policies) {
            return;
        }
    }
    matched_database_table_policies tmp_policies;
    auto tmp_policies_str = policies;
    dsn::json::json_forwarder<matched_database_table_policies>::decode(
        dsn::blob::create_from_bytes(std::move(tmp_policies_str)), tmp_policies);
    {
        utils::auto_write_lock l(_lock);
        _env_policies = policies;
        _ranger_policies = std::move(tmp_policies);
    }
}

void replica_access_controller::check_allowed_users_valid() const
{
    LOG_WARNING_IF(
        FLAGS_enable_acl && !FLAGS_enable_ranger_acl && _allowed_users.empty(),
        "Without specifying any ACL, the table is not really protected, please check in time.");
}

} // namespace security
} // namespace dsn

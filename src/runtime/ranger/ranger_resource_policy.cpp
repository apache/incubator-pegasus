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

#include "ranger_resource_policy.h"

#include "runtime/ranger/access_type.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace ranger {

bool policy_item::match(const access_type &ac_type, const std::string &user_name) const
{
    return static_cast<bool>(access_types & ac_type) && users.count(user_name) != 0;
}

policy_check_status acl_policies::policies_check(const access_type &ac_type,
                                                 const std::string &user_name,
                                                 const policy_check_type &check_type) const
{
    if (check_type == policy_check_type::kAllow) {
        return do_policies_check(
            check_type, ac_type, user_name, allow_policies, allow_policies_exclude);
    }
    CHECK(check_type == policy_check_type::kDeny, "");
    return do_policies_check(check_type, ac_type, user_name, deny_policies, deny_policies_exclude);
}

policy_check_status
acl_policies::do_policies_check(const policy_check_type &check_type,
                                const access_type &ac_type,
                                const std::string &user_name,
                                const std::vector<policy_item> &policies,
                                const std::vector<policy_item> &exclude_policies) const
{
    for (const auto &policy : policies) {
        // 1. Doesn't match an allow_policies or a deny_policies.
        if (!policy.match(ac_type, user_name)) {
            continue;
        }
        // 2. Matches a policy.
        for (const auto &exclude_policy : exclude_policies) {
            if (exclude_policy.match(ac_type, user_name)) {
                // 2.1. Matches an allow/deny_policies_exclude.
                return policy_check_status::kPending;
            }
        }
        // 2.2. Doesn't match any allow/deny_exclude_policies.
        if (check_type == policy_check_type::kAllow) {
            return policy_check_status::kAllowed;
        } else {
            return policy_check_status::kDenied;
        }
    }
    // 3. Doesn't match any policy.
    return policy_check_status::kNotMatched;
}

access_control_result
check_ranger_resource_policy_allowed(const std::vector<ranger_resource_policy> &policies,
                                     const access_type &ac_type,
                                     const std::string &user_name,
                                     const match_database_type &md_type,
                                     const std::string &database_name,
                                     const std::string &default_database_name)
{
    // Check if it is denied by any policy in current resource.
    auto check_res = do_check_ranger_resource_policy(policies,
                                                     ac_type,
                                                     user_name,
                                                     md_type,
                                                     database_name,
                                                     default_database_name,
                                                     policy_check_type::kDeny);
    if (access_control_result::kDenied == check_res) {
        return access_control_result::kDenied;
    }

    // Check if it is allowed by any policy in current resource.
    check_res = do_check_ranger_resource_policy(policies,
                                                ac_type,
                                                user_name,
                                                md_type,
                                                database_name,
                                                default_database_name,
                                                policy_check_type::kAllow);
    if (access_control_result::kAllowed == check_res) {
        return access_control_result::kAllowed;
    }

    // The check that does not match any policy in current reosource returns false.
    return access_control_result::kDenied;
}

access_control_result
do_check_ranger_resource_policy(const std::vector<ranger_resource_policy> &policies,
                                const access_type &ac_type,
                                const std::string &user_name,
                                const match_database_type &md_type,
                                const std::string &database_name,
                                const std::string &default_database_name,
                                const policy_check_type &check_type)
{
    for (const auto &policy : policies) {
        if (match_database_type::kNeed == md_type) {
            // Lagacy table not match any database.
            if (database_name.empty() && policy.database_names.count("*") == 0 &&
                policy.database_names.count(default_database_name) == 0) {
                continue;
            }
            // New table not match any database.
            if (!database_name.empty() && policy.database_names.count("*") == 0 &&
                policy.database_names.count(database_name) == 0) {
                continue;
            }
        }
        auto check_status = policy.policies.policies_check(ac_type, user_name, check_type);
        // When policy_check_type is 'kDeny' and in a 'deny_policies' and not in any
        // 'deny_policies_exclude'.
        if (policy_check_type::kDeny == check_type &&
            policy_check_status::kDenied == check_status) {
            return access_control_result::kDenied;
        }
        // When policy_check_type is 'kAllow' and in a 'allow_policies' and not in any
        // 'allow_policies_exclude'.
        if (policy_check_type::kAllow == check_type &&
            policy_check_status::kAllowed == check_status) {
            return access_control_result::kAllowed;
        }
        // In a 'policies' and in a 'policies_exclude' or not match.
        if (policy_check_status::kPending == check_status ||
            policy_check_status::kNotMatched == check_status) {
            continue;
        }
    }
    return access_control_result::kPending;
}

access_control_result check_ranger_database_table_policy_allowed(
    const std::vector<matched_database_table_policy> &policies,
    const access_type &ac_type,
    const std::string &user_name)
{
    // Check if it is denied by any DATABASE_TABLE policy.
    auto check_res = do_check_ranger_database_table_policy(
        policies, ac_type, user_name, policy_check_type::kDeny);
    if (access_control_result::kDenied == check_res) {
        return access_control_result::kDenied;
    }

    // Check if it is allowed by any DATABASE_TABLE policy.
    check_res = do_check_ranger_database_table_policy(
        policies, ac_type, user_name, policy_check_type::kAllow);
    if (access_control_result::kAllowed == check_res) {
        return access_control_result::kAllowed;
    }

    // The check that does not match any DATABASE_TABLE policy returns false.
    return access_control_result::kDenied;
}

access_control_result
do_check_ranger_database_table_policy(const std::vector<matched_database_table_policy> &policies,
                                      const access_type &ac_type,
                                      const std::string &user_name,
                                      const policy_check_type &check_type)
{
    for (const auto &policy : policies) {
        auto check_status = policy.policies.policies_check(ac_type, user_name, check_type);
        // When policy_check_type is 'kDeny' and in a 'deny_policies' and not in any
        // 'deny_policies_exclude'.
        if (policy_check_type::kDeny == check_type &&
            policy_check_status::kDenied == check_status) {
            return access_control_result::kDenied;
        }
        // When policy_check_type is 'kAllow' and in a 'allow_policies' and not in any
        // 'allow_policies_exclude'.
        if (policy_check_type::kAllow == check_type &&
            policy_check_status::kAllowed == check_status) {
            return access_control_result::kAllowed;
        }
        // In a 'policies' and in a 'policies_exclude' or not match.
        if (policy_check_status::kPending == check_status ||
            policy_check_status::kNotMatched == check_status) {
            continue;
        }
    }
    return access_control_result::kPending;
}

} // namespace ranger
} // namespace dsn

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

#include "gutil/map_util.h"
#include "ranger/access_type.h"
#include "utils/fmt_logging.h"

namespace dsn::ranger {

bool policy_item::match(access_type ac_type, const std::string &user_name) const
{
    return static_cast<bool>(access_types & ac_type) && gutil::ContainsKey(users, user_name);
}

template <>
policy_check_status
acl_policies::policies_check<policy_check_type::kAllow>(access_type ac_type,
                                                        const std::string &user_name) const
{
    return do_policies_check<policy_check_type::kAllow, policy_check_status::kAllowed>(ac_type,
                                                                                       user_name);
}

template <>
policy_check_status
acl_policies::policies_check<policy_check_type::kDeny>(access_type ac_type,
                                                       const std::string &user_name) const
{
    return do_policies_check<policy_check_type::kDeny, policy_check_status::kDenied>(ac_type,
                                                                                     user_name);
}

template <>
policy_check_status
acl_policies::do_policies_check<policy_check_type::kAllow, policy_check_status::kAllowed>(
    access_type ac_type, const std::string &user_name) const
{
    for (const auto &policy : allow_policies) {
        // 1. Doesn't match an allow_policies.
        if (!policy.match(ac_type, user_name)) {
            continue;
        }
        // 2. Matches a policy.
        for (const auto &exclude_policy : allow_policies_exclude) {
            if (exclude_policy.match(ac_type, user_name)) {
                // 2.1. Matches an allow_policies_exclude.
                return policy_check_status::kPending;
            }
        }
        // 2.2. Doesn't match any allow_exclude_policies.
        return policy_check_status::kAllowed;
    }
    // 3. Doesn't match any policy.
    return policy_check_status::kNotMatched;
}

template <>
policy_check_status
acl_policies::do_policies_check<policy_check_type::kDeny, policy_check_status::kDenied>(
    access_type ac_type, const std::string &user_name) const
{
    for (const auto &policy : deny_policies) {
        // 1. Doesn't match a deny_policies.
        if (!policy.match(ac_type, user_name)) {
            continue;
        }
        // 2. Matches a policy.
        for (const auto &exclude_policy : deny_policies_exclude) {
            if (exclude_policy.match(ac_type, user_name)) {
                // 2.1. Matches a deny_policies_exclude.
                return policy_check_status::kPending;
            }
        }
        // 2.2. Doesn't match any deny_exclude_policies.
        return policy_check_status::kDenied;
    }
    // 3. Doesn't match any policy.
    return policy_check_status::kNotMatched;
}

access_control_result
check_ranger_resource_policy_allowed(const std::vector<ranger_resource_policy> &policies,
                                     access_type ac_type,
                                     const std::string &user_name,
                                     match_database_type md_type,
                                     const std::string &database_name,
                                     const std::string &default_database_name)
{
    // Check if it is denied by any policy in current resource.
    auto check_res = do_check_ranger_resource_policy<policy_check_type::kDeny>(
        policies, ac_type, user_name, md_type, database_name, default_database_name);
    if (access_control_result::kDenied == check_res) {
        return access_control_result::kDenied;
    }
    CHECK(access_control_result::kPending == check_res, "the access control result must kPending.");

    // Check if it is allowed by any policy in current resource.
    check_res = do_check_ranger_resource_policy<policy_check_type::kAllow>(
        policies, ac_type, user_name, md_type, database_name, default_database_name);
    if (access_control_result::kAllowed == check_res) {
        return access_control_result::kAllowed;
    }
    CHECK(access_control_result::kPending == check_res, "the access control result must kPending.");

    // The check that does not match any policy in current reosource returns false.
    return access_control_result::kDenied;
}

namespace {

bool match_database_name(match_database_type md_type,
                         const ranger_resource_policy &policy,
                         const std::string &database_name,
                         const std::string &default_database_name)
{
    if (md_type != match_database_type::kNeed) {
        // No need to match.
        return true;
    }

    if (gutil::ContainsKey(policy.database_names, "*")) {
        // An asterisk(*) matches any database name.
        return true;
    }

    // `default_database_name` is used for the lagacy table name, which does not include the
    // part of the database name.
    const std::string &check_name = database_name.empty() ? default_database_name : database_name;
    return gutil::ContainsKey(policy.database_names, check_name);
}

} // anonymous namespace

template <>
access_control_result do_check_ranger_resource_policy<policy_check_type::kAllow>(
    const std::vector<ranger_resource_policy> &policies,
    access_type ac_type,
    const std::string &user_name,
    match_database_type md_type,
    const std::string &database_name,
    const std::string &default_database_name)
{
    for (const auto &policy : policies) {
        if (!match_database_name(md_type, policy, database_name, default_database_name)) {
            continue;
        }

        const auto check_status =
            policy.policies.policies_check<policy_check_type::kAllow>(ac_type, user_name);
        if (check_status == policy_check_status::kAllowed) {
            return access_control_result::kAllowed;
        }

        // In a 'allow_policies' and in a 'allow_policies_exclude' or not match.
        CHECK(check_status == policy_check_status::kPending ||
                  check_status == policy_check_status::kNotMatched,
              "the policy check status must be kPending or kNotMatched");
    }

    return access_control_result::kPending;
}

template <>
access_control_result do_check_ranger_resource_policy<policy_check_type::kDeny>(
    const std::vector<ranger_resource_policy> &policies,
    access_type ac_type,
    const std::string &user_name,
    match_database_type md_type,
    const std::string &database_name,
    const std::string &default_database_name)
{
    for (const auto &policy : policies) {
        if (!match_database_name(md_type, policy, database_name, default_database_name)) {
            continue;
        }

        const auto check_status =
            policy.policies.policies_check<policy_check_type::kDeny>(ac_type, user_name);
        if (check_status == policy_check_status::kDenied) {
            return access_control_result::kDenied;
        }

        // In a 'deny_policies' and in a 'deny_policies_exclude' or not match.
        CHECK(check_status == policy_check_status::kPending ||
                  check_status == policy_check_status::kNotMatched,
              "the policy check status must be kPending or kNotMatched");
    }

    return access_control_result::kPending;
}

access_control_result check_ranger_database_table_policy_allowed(
    const std::vector<matched_database_table_policy> &policies,
    access_type ac_type,
    const std::string &user_name)
{
    // Check if it is denied by any DATABASE_TABLE policy.
    auto check_res = do_check_ranger_database_table_policy<policy_check_type::kDeny>(
        policies, ac_type, user_name);
    if (access_control_result::kDenied == check_res) {
        return access_control_result::kDenied;
    }
    CHECK(access_control_result::kPending == check_res, "the access control result must kPending.");

    // Check if it is allowed by any DATABASE_TABLE policy.
    check_res = do_check_ranger_database_table_policy<policy_check_type::kAllow>(
        policies, ac_type, user_name);
    if (access_control_result::kAllowed == check_res) {
        return access_control_result::kAllowed;
    }
    CHECK(access_control_result::kPending == check_res, "the access control result must kPending.");

    // The check that does not match any DATABASE_TABLE policy returns false.
    return access_control_result::kDenied;
}

template <>
access_control_result do_check_ranger_database_table_policy<policy_check_type::kDeny>(
    const std::vector<matched_database_table_policy> &policies,
    access_type ac_type,
    const std::string &user_name)
{
    for (const auto &policy : policies) {
        auto check_status =
            policy.policies.policies_check<policy_check_type::kDeny>(ac_type, user_name);
        // When policy_check_type is 'kDeny' and in a 'deny_policies' and not in any
        // 'deny_policies_exclude'.
        if (policy_check_status::kDenied == check_status) {
            return access_control_result::kDenied;
        }

        // In a 'policies' and in a 'policies_exclude' or not match.
        CHECK(policy_check_status::kPending == check_status ||
                  policy_check_status::kNotMatched == check_status,
              "the policy check status must be kPending or kNotMatched");
    }
    return access_control_result::kPending;
}

template <>
access_control_result do_check_ranger_database_table_policy<policy_check_type::kAllow>(
    const std::vector<matched_database_table_policy> &policies,
    access_type ac_type,
    const std::string &user_name)
{
    for (const auto &policy : policies) {
        auto check_status =
            policy.policies.policies_check<policy_check_type::kAllow>(ac_type, user_name);
        // When policy_check_type is 'kAllow' and in a 'allow_policies' and not in any
        // 'allow_policies_exclude'.
        if (policy_check_status::kAllowed == check_status) {
            return access_control_result::kAllowed;
        }
        // In a 'policies' and in a 'policies_exclude' or not match.
        CHECK(policy_check_status::kPending == check_status ||
                  policy_check_status::kNotMatched == check_status,
              "the policy check status must be kPending or kNotMatched");
    }
    return access_control_result::kPending;
}

} // namespace dsn::ranger

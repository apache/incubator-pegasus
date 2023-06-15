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

#include "access_type.h"
#include "common/json_helper.h"
#include "utils/enum_helper.h"

namespace dsn {
namespace ranger {

// Types of policy checks.
// kAllow means this checks for 'allow_policies' and 'allow_policies_exclude'.
// kDeny means this checks for 'deny_policies' and 'deny_policies_exclude'.
enum class policy_check_type
{
    kAllow = 0,
    kDeny,
    kInvalid
};
ENUM_BEGIN(policy_check_type, policy_check_type::kInvalid)
ENUM_REG(policy_check_type::kAllow)
ENUM_REG(policy_check_type::kDeny)
ENUM_END(policy_check_type)

// The return status code when a policy('kAllow' or 'kDeny' policy_check_type) is checked.
// kAllowed means in a 'allow_policies' and not in any 'allow_policies_exclude'.
// kDenied means in a 'deny_policies' and not in any 'deny_policies_exclude'.
// kNotMatched means not match any 'allow_policies' or 'deny_policies'.
// kPending means in a 'allow_policies/deny_policies' and in a
// 'allow_policies_exclude/deny_policies_exclude'.
enum class policy_check_status
{
    kAllowed = 0,
    kDenied,
    kNotMatched,
    kPending,
    kInvalid
};

enum class access_control_result
{
    kAllowed = 0,
    kDenied,
    kPending
};

// Used to determine whether the policy needs to match the database. kNotNeed means no, kNeed means
// yes.
enum class match_database_type
{
    kNotNeed = 0,
    kNeed

};

// Ranger policy data structure
struct policy_item
{
    access_type access_types = access_type::kInvalid;
    std::unordered_set<std::string> users;

    DEFINE_JSON_SERIALIZATION(access_types, users);

    // Check if the 'acl_type' - 'user_name' pair is matched to the policy.
    // Return true if it is matched, otherwise return false.
    // TODO(wanghao): add benchmark test
    bool match(const access_type &ac_type, const std::string &user_name) const;
};

// Data structure of policies with different priorities
struct acl_policies
{
    // policy priority: deny_policies_exclude > deny_policies > allow_policies_exclude >
    // allow_policies
    std::vector<policy_item> allow_policies;
    std::vector<policy_item> allow_policies_exclude;
    std::vector<policy_item> deny_policies;
    std::vector<policy_item> deny_policies_exclude;

    DEFINE_JSON_SERIALIZATION(allow_policies,
                              allow_policies_exclude,
                              deny_policies,
                              deny_policies_exclude);

    // Check if 'allow_policies' or 'deny_policies' allow or deny 'user_name' to access the resource
    // by type 'ac_type'.
    template <policy_check_type check_type>
    policy_check_status policies_check(const access_type &ac_type,
                                       const std::string &user_name) const;

    template <policy_check_type check_type, policy_check_status check_status>
    policy_check_status do_policies_check(const access_type &ac_type,
                                          const std::string &user_name) const;
};

template <>
policy_check_status
acl_policies::policies_check<policy_check_type::kAllow>(const access_type &ac_type,
                                                        const std::string &user_name) const;

template <>
policy_check_status
acl_policies::policies_check<policy_check_type::kDeny>(const access_type &ac_type,
                                                       const std::string &user_name) const;

template <>
policy_check_status
acl_policies::do_policies_check<policy_check_type::kAllow, policy_check_status::kAllowed>(
    const access_type &ac_type, const std::string &user_name) const;

template <>
policy_check_status
acl_policies::do_policies_check<policy_check_type::kDeny, policy_check_status::kDenied>(
    const access_type &ac_type, const std::string &user_name) const;

// A policy data structure definition of ranger resources
struct ranger_resource_policy
{
    std::string name;
    std::unordered_set<std::string> database_names;
    std::unordered_set<std::string> table_names;
    acl_policies policies;

    DEFINE_JSON_SERIALIZATION(name, database_names, table_names, policies);
};

// A policy data structure definition of the DATABASE_TABLE resource, which will be set in
// 'app_envs'
struct matched_database_table_policy
{
    std::string matched_database_name;
    std::string matched_table_name;
    acl_policies policies;

    DEFINE_JSON_SERIALIZATION(matched_database_name, matched_table_name, policies);
};

// Returns 'access_control_result::kAllowed' if 'policies' allows 'user_name' to access
// 'database_name' via 'ac_type', returns 'access_control_result::kDenied' means not.
// 'need_match_database' being true means that the 'policies' needs to be matched to the database
// first, false means not.
// If 'ac_type' is DATABASE access type, it needs to match database, if 'ac_type' is a GLOBAL access
// type, it does not need to match.
/*
                *** Ranger Policy Evaluation Flow ***

                    +-----------------+
                     \ Resource access \
                      \    request      \
                       +-------+---------+
                               |
                         +-----v-------+
                        /               \
                       /     Has a       \
         +-----N------+  resource policy  <----------------N-----------------+
         |             \  been matched ? /                                   |
         |              \               /                                    |
         |               +-----+-------+                                     |
         |                     |                                             |
         |                     Y                                             |
         |                     |                                             |
         |               +-----v-------+                  +-------------+    |
         |              /               \                /               \   |
         |             /    Has more     \              /   Has more      \  |
         |      +----->   policies with   +---N--+---->+  policies with    +-+
         |      |      \ Deny Condition? /       |      \ Allow Condition?/
         |      |       \               /        |       \               /
         |      |        +------+------+         |        +------+------+
         |      |               |                |               |
         |      |               Y                |               Y
         |      |               |                |               |
         |      |        +------v------+         |        +------v------+
         |      |       /    Request    \        |       /    Request    \
         |      |      / matches a deny  \       |      /matches an allow \
         |      +--N--+ condition in the  +      +--N--+ condition in the  +
         |      |      \     policy?     /       |      \    policy?      /
         |      |       \               /        |       \               /
         |      |        +------+------+         |        +------+------+
         |      |               |                |               |
         |      |               Y                |               Y
         |      |               |                |               |
         |      |        +------v------+         |        +------v------+
         |      |       /    Request    \        |       /    Request    \
         |      |      /  matches a deny \       |      / matches an allow\
         |      +--Y--+   exclude in the  +      +--Y--+   exclude in the  +
         |             \      policy?    /              \      policy?    /
         |              \               /                \               /
         |               +------+------+                  +------+------+
         |                      |                                |
         |                      N                                N
         |                      |                                |
   +-----v-----+         +------v------+                  +------v------+
   |    DENY   |         |    DENY     |                  |    ALLOW    |
   +-----------+         +-------------+                  +-------------+
*/
access_control_result
check_ranger_resource_policy_allowed(const std::vector<ranger_resource_policy> &policies,
                                     const access_type &ac_type,
                                     const std::string &user_name,
                                     const match_database_type &md_type,
                                     const std::string &database_name,
                                     const std::string &default_database_name);

template <policy_check_type check_type>
access_control_result
do_check_ranger_resource_policy(const std::vector<ranger_resource_policy> &policies,
                                const access_type &ac_type,
                                const std::string &user_name,
                                const match_database_type &md_type,
                                const std::string &database_name,
                                const std::string &default_database_name);

template <>
access_control_result do_check_ranger_resource_policy<policy_check_type::kAllow>(
    const std::vector<ranger_resource_policy> &policies,
    const access_type &ac_type,
    const std::string &user_name,
    const match_database_type &md_type,
    const std::string &database_name,
    const std::string &default_database_name);

template <>
access_control_result do_check_ranger_resource_policy<policy_check_type::kDeny>(
    const std::vector<ranger_resource_policy> &policies,
    const access_type &ac_type,
    const std::string &user_name,
    const match_database_type &md_type,
    const std::string &database_name,
    const std::string &default_database_name);

// Return 'access_control_result::kAllowed' if 'policies' allow 'user_name' to access, this is used
// for DATABASE_TABLE resource, returns 'access_control_result::kDenied' means not.
access_control_result check_ranger_database_table_policy_allowed(
    const std::vector<matched_database_table_policy> &policies,
    const access_type &ac_type,
    const std::string &user_name);

template <policy_check_type check_type>
access_control_result
do_check_ranger_database_table_policy(const std::vector<matched_database_table_policy> &policies,
                                      const access_type &ac_type,
                                      const std::string &user_name);

template <>
access_control_result do_check_ranger_database_table_policy<policy_check_type::kDeny>(
    const std::vector<matched_database_table_policy> &policies,
    const access_type &ac_type,
    const std::string &user_name);

template <>
access_control_result do_check_ranger_database_table_policy<policy_check_type::kAllow>(
    const std::vector<matched_database_table_policy> &policies,
    const access_type &ac_type,
    const std::string &user_name);

} // namespace ranger
} // namespace dsn

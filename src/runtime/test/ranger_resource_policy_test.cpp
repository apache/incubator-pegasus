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

#include <fmt/core.h>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "runtime/ranger/access_type.h"
#include "runtime/ranger/ranger_resource_policy.h"

namespace dsn {
namespace ranger {

TEST(ranger_resource_policy_test, policy_item_match)
{
    policy_item item = {access_type::kRead | access_type::kWrite | access_type::kCreate,
                        {"user1", "user2"}};
    struct test_case
    {
        access_type ac_type;
        std::string user_name;
        bool expected_result;
    } tests[] = {{access_type::kRead, "", false},
                 {access_type::kRead, "user", false},
                 {access_type::kRead, "user1", true},
                 {access_type::kWrite, "user1", true},
                 {access_type::kCreate, "user1", true},
                 {access_type::kDrop, "user1", false},
                 {access_type::kList, "user1", false},
                 {access_type::kMetadata, "user1", false},
                 {access_type::kControl, "user1", false},
                 {access_type::kWrite, "user2", true}};
    for (const auto &test : tests) {
        auto actual_result = item.match(test.ac_type, test.user_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}

TEST(ranger_resource_policy_test, acl_policies_allowed)
{
    acl_policies policy;
    policy.allow_policies = {{access_type::kRead | access_type::kWrite | access_type::kCreate,
                              {"user1", "user2", "user3"}},
                             {access_type::kRead | access_type::kWrite | access_type::kCreate,
                              {"user4", "user5", "user6"}}};
    policy.allow_policies_exclude = {{access_type::kWrite | access_type::kCreate, {"user2"}},
                                     {access_type::kWrite | access_type::kCreate, {"user5"}}};
    policy.deny_policies = {{access_type::kRead | access_type::kWrite, {"user3", "user4"}},
                            {access_type::kRead | access_type::kWrite, {"user5", "user6"}}};
    policy.deny_policies_exclude = {{access_type::kRead, {"user4"}},
                                    {access_type::kWrite, {"user6"}}};
    struct test_case
    {
        access_type ac_type;
        std::string user_name;
        policy_check_type check_type;
        policy_check_status expected_result;
    } tests[] = {
        // not in any "allow_policies"
        {access_type::kRead, "user", policy_check_type::kAllow, policy_check_status::kNotMatched},
        // not in any "deny_policies"
        {access_type::kRead, "user", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kRead, "user1", policy_check_type::kAllow, policy_check_status::kAllowed},
        // not in any 'deny_policies'
        {access_type::kRead, "user1", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // not in any "allow_policies"
        {access_type::kList, "user1", policy_check_type::kAllow, policy_check_status::kNotMatched},
        // not in any "deny_policies"
        {access_type::kList, "user1", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kRead, "user2", policy_check_type::kAllow, policy_check_status::kAllowed},
        // not in any "deny_policies"
        {access_type::kRead, "user2", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and in a 'allow_policies_exclude'
        {access_type::kWrite, "user2", policy_check_type::kAllow, policy_check_status::kPending},
        // not in any "deny_policies"
        {access_type::kWrite, "user2", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and in a 'allow_policies_exclude'
        {access_type::kCreate, "user2", policy_check_type::kAllow, policy_check_status::kPending},
        // not in any "deny_policies"
        {access_type::kCreate, "user2", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kRead, "user3", policy_check_type::kAllow, policy_check_status::kAllowed},
        // in a 'deny_policies' and not in any 'deny_policies_exclude'
        {access_type::kRead, "user3", policy_check_type::kDeny, policy_check_status::kDenied},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kCreate, "user3", policy_check_type::kAllow, policy_check_status::kAllowed},
        // not in any "deny_policies"
        {access_type::kCreate, "user3", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // not in any "allow_policies"
        {access_type::kList, "user3", policy_check_type::kAllow, policy_check_status::kNotMatched},
        // not in any "deny_policies"
        {access_type::kList, "user3", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kRead, "user4", policy_check_type::kAllow, policy_check_status::kAllowed},
        // in a 'deny_policies' and in a 'deny_policies_exclude'
        {access_type::kRead, "user4", policy_check_type::kDeny, policy_check_status::kPending},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kWrite, "user4", policy_check_type::kAllow, policy_check_status::kAllowed},
        // in a 'deny_policies' and not in any 'deny_policies_exclude'
        {access_type::kWrite, "user4", policy_check_type::kDeny, policy_check_status::kDenied},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kCreate, "user4", policy_check_type::kAllow, policy_check_status::kAllowed},
        // not in any "deny_policies"
        {access_type::kCreate, "user4", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // not in any "allow_policies"
        {access_type::kList, "user4", policy_check_type::kAllow, policy_check_status::kNotMatched},
        // not in any "deny_policies"
        {access_type::kList, "user4", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kRead, "user5", policy_check_type::kAllow, policy_check_status::kAllowed},
        // in a 'deny_policies' and  not in any 'deny_policies_exclude'
        {access_type::kRead, "user5", policy_check_type::kDeny, policy_check_status::kDenied},
        // in a 'allow_policies' and in a 'allow_policies_exclude'
        {access_type::kWrite, "user5", policy_check_type::kAllow, policy_check_status::kPending},
        // in a 'deny_policies' and not in any 'deny_policies_exclude'
        {access_type::kWrite, "user5", policy_check_type::kDeny, policy_check_status::kDenied},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kCreate, "user5", policy_check_type::kAllow, policy_check_status::kPending},
        // not in any "deny_policies"
        {access_type::kCreate, "user5", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // not in any "allow_policies"
        {access_type::kList, "user5", policy_check_type::kAllow, policy_check_status::kNotMatched},
        // not in any "deny_policies"
        {access_type::kList, "user5", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kRead, "user6", policy_check_type::kAllow, policy_check_status::kAllowed},
        // in a 'deny_policies' and  not in any 'deny_policies_exclude'
        {access_type::kRead, "user6", policy_check_type::kDeny, policy_check_status::kDenied},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kWrite, "user6", policy_check_type::kAllow, policy_check_status::kAllowed},
        // in a 'deny_policies' and in a 'deny_policies_exclude'
        {access_type::kWrite, "user6", policy_check_type::kDeny, policy_check_status::kPending},
        // in a 'allow_policies' and not in any 'allow_policies_exclude'
        {access_type::kCreate, "user6", policy_check_type::kAllow, policy_check_status::kAllowed},
        // not in any "deny_policies"
        {access_type::kCreate, "user6", policy_check_type::kDeny, policy_check_status::kNotMatched},
        // not in any "allow_policies"
        {access_type::kList, "user6", policy_check_type::kAllow, policy_check_status::kNotMatched},
        // not in any "deny_policies"
        {access_type::kList, "user6", policy_check_type::kDeny, policy_check_status::kNotMatched},
    };
    for (const auto &test : tests) {
        policy_check_status actual_result = policy_check_status::kInvalid;
        switch (test.check_type) {
        case policy_check_type::kAllow:
            actual_result =
                policy.policies_check<policy_check_type::kAllow>(test.ac_type, test.user_name);
            break;
        case policy_check_type::kDeny:
            actual_result =
                policy.policies_check<policy_check_type::kDeny>(test.ac_type, test.user_name);
            break;
        case policy_check_type::kInvalid:
        default:
            break;
        }
        EXPECT_EQ(test.expected_result, actual_result)
            << fmt::format("ac_type: {}, user_name: {}, check_type: {}",
                           enum_to_string(test.ac_type),
                           test.user_name,
                           enum_to_string(test.check_type));
    }
}
} // namespace ranger
} // namespace dsn

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

#include <gtest/gtest.h>

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
                              {"user1", "user2", "user3", "user4"}}};
    policy.allow_policies_exclude = {{access_type::kWrite | access_type::kCreate, {"user2"}}};
    policy.deny_policies = {{access_type::kRead | access_type::kWrite, {"user3", "user4"}}};
    policy.deny_policies_exclude = {{access_type::kRead, {"user4"}}};
    struct test_case
    {
        access_type ac_type;
        std::string user_name;
        bool expected_result;
    } tests[] = {{access_type::kRead, "user", false},      {access_type::kRead, "user1", true},
                 {access_type::kWrite, "user1", true},     {access_type::kCreate, "user1", true},
                 {access_type::kDrop, "user1", false},     {access_type::kList, "user1", false},
                 {access_type::kMetadata, "user1", false}, {access_type::kControl, "user1", false},
                 {access_type::kRead, "user2", true},      {access_type::kWrite, "user2", false},
                 {access_type::kCreate, "user2", false},   {access_type::kDrop, "user2", false},
                 {access_type::kList, "user2", false},     {access_type::kMetadata, "user2", false},
                 {access_type::kControl, "user2", false},  {access_type::kRead, "user3", false},
                 {access_type::kCreate, "user3", true},    {access_type::kList, "user3", false},
                 {access_type::kRead, "user4", true},      {access_type::kWrite, "user4", false},
                 {access_type::kCreate, "user4", true},    {access_type::kList, "user4", false}};
    for (const auto &test : tests) {
        auto actual_result = policy.allowed(test.ac_type, test.user_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}
} // namespace ranger
} // namespace dsn

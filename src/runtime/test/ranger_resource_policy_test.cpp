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
    policy_item item = {KRead | KWrite | KCreate, {"user1", "user2"}};
    struct test_case
    {
        access_type ac_type;
        std::string user_name;
        bool expected_result;
    } tests[] = {{KRead, "", false},
                 {KRead, "user", false},
                 {KRead, "user1", true},
                 {KWrite, "user1", true},
                 {KCreate, "user1", true},
                 {KDrop, "user1", false},
                 {KList, "user1", false},
                 {KMetadata, "user1", false},
                 {KControl, "user1", false},
                 {KWrite, "user2", true}};
    for (const auto &test : tests) {
        auto actual_result = item.match(test.ac_type, test.user_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}

TEST(ranger_resource_policy_test, acl_policies_allowed)
{
    acl_policies policy;
    policy.allow_policies = {{KRead | KWrite | KCreate, {"user1", "user2", "user3", "user4"}}};
    policy.allow_policies_exclude = {{KWrite | KCreate, {"user2"}}};
    policy.deny_policies = {{KRead | KWrite, {"user3", "user4"}}};
    policy.deny_policies_exclude = {{KRead, {"user4"}}};
    struct test_case
    {
        access_type ac_type;
        std::string user_name;
        bool expected_result;
    } tests[] = {
        {KRead, "user", false},      {KRead, "user1", true},      {KWrite, "user1", true},
        {KCreate, "user1", true},    {KDrop, "user1", false},     {KList, "user1", false},
        {KMetadata, "user1", false}, {KControl, "user1", false},  {KRead, "user2", true},
        {KWrite, "user2", false},    {KCreate, "user2", false},   {KDrop, "user2", false},
        {KList, "user2", false},     {KMetadata, "user2", false}, {KControl, "user2", false},
        {KRead, "user3", false},     {KCreate, "user3", true},    {KList, "user3", false},
        {KRead, "user4", true},      {KWrite, "user4", false},    {KCreate, "user4", true},
        {KList, "user4", false}};
    for (const auto &test : tests) {
        auto actual_result = policy.allowed(test.ac_type, test.user_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}
} // namespace ranger
} // namespace dsn

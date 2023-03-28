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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/json_helper.h"
#include "runtime/ranger/access_type.h"
#include "runtime/ranger/ranger_resource_policy.h"
#include "runtime/ranger/ranger_resource_policy_manager.h"
#include "utils/blob.h"

namespace dsn {
namespace ranger {

TEST(ranger_resource_policy_manager_test, parse_policies_from_json_for_test)
{
    std::string data = R"(
        [{
	        "accesses": [{
		        "type": "create",
		        "isAllowed": true
	        }, {
		        "type": "drop",
		        "isAllowed": true
	        }, {
		        "type": "control",
		        "isAllowed": true
	        }, {
		        "type": "metadata",
		        "isAllowed": true
	        }, {
		        "type": "list",
		        "isAllowed": true
	        }, {
		        "type": "fake",
		        "isAllowed": true
	        }, {
		        "type": "read",
		        "isAllowed": false
	        }],
	        "users": ["user1", "user2"],
	        "groups": [],
	        "roles": [],
	        "conditions": [],
	        "delegateAdmin": true
        }, {
	        "accesses": [{
		        "type": "read",
		        "isAllowed": true
	        }, {
		        "type": "write",
		        "isAllowed": true
	        }],
	        "users": ["user2"],
	        "groups": [],
	        "roles": [],
	        "conditions": [],
	        "delegateAdmin": true
        }]
    )";

    std::vector<policy_item> fake_policies;

    rapidjson::Document fake_doc;
    fake_doc.Parse(data.c_str());
    ranger_resource_policy_manager::parse_policies_from_json(fake_doc, fake_policies);

    EXPECT_EQ(2, fake_policies.size());

    ASSERT_EQ(access_type::kCreate | access_type::kDrop | access_type::kList |
                  access_type::kMetadata | access_type::kControl,
              fake_policies[0].access_types);

    ASSERT_EQ(access_type::kRead | access_type::kWrite, fake_policies[1].access_types);

    struct test_case
    {
        policy_item item;
        access_type ac_type;
        std::string user_name;
        bool expected_result;
    } tests[] = {{fake_policies[0], access_type::kRead, "", false},
                 {fake_policies[0], access_type::kRead, "user", false},
                 {fake_policies[0], access_type::kRead, "user1", false},
                 {fake_policies[0], access_type::kWrite, "user1", false},
                 {fake_policies[0], access_type::kCreate, "user1", true},
                 {fake_policies[0], access_type::kDrop, "user1", true},
                 {fake_policies[0], access_type::kList, "user1", true},
                 {fake_policies[0], access_type::kMetadata, "user1", true},
                 {fake_policies[0], access_type::kControl, "user1", true},
                 {fake_policies[0], access_type::kRead, "user2", false},
                 {fake_policies[0], access_type::kWrite, "user2", false},
                 {fake_policies[0], access_type::kCreate, "user2", true},
                 {fake_policies[0], access_type::kDrop, "user2", true},
                 {fake_policies[0], access_type::kList, "user2", true},
                 {fake_policies[0], access_type::kMetadata, "user2", true},
                 {fake_policies[0], access_type::kControl, "user2", true},
                 {fake_policies[1], access_type::kRead, "user1", false},
                 {fake_policies[1], access_type::kWrite, "user1", false},
                 {fake_policies[1], access_type::kCreate, "user1", false},
                 {fake_policies[1], access_type::kDrop, "user1", false},
                 {fake_policies[1], access_type::kList, "user1", false},
                 {fake_policies[1], access_type::kMetadata, "user1", false},
                 {fake_policies[1], access_type::kControl, "user1", false},
                 {fake_policies[1], access_type::kRead, "user2", true},
                 {fake_policies[1], access_type::kWrite, "user2", true},
                 {fake_policies[1], access_type::kCreate, "user2", false},
                 {fake_policies[1], access_type::kDrop, "user2", false},
                 {fake_policies[1], access_type::kList, "user2", false},
                 {fake_policies[1], access_type::kMetadata, "user2", false},
                 {fake_policies[1], access_type::kControl, "user2", false}};
    for (const auto &test : tests) {
        auto actual_result = test.item.match(test.ac_type, test.user_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}

// Check whether 'all_resource_policies' can correctly decode and encode
TEST(ranger_resource_policy_manager_test, ranger_resource_policy_serialized_test)
{
    // 1. Create a fake resource policies data in 'fake_all_resource_policies'
    acl_policies fake_policy;
    fake_policy.allow_policies = {{access_type::kRead | access_type::kWrite | access_type::kList,
                                   {"user1", "user2", "user3", "user4"}}};
    fake_policy.allow_policies_exclude = {{access_type::kWrite | access_type::kCreate, {"user2"}}};
    fake_policy.deny_policies = {{access_type::kRead | access_type::kWrite, {"user3", "user4"}}};
    fake_policy.deny_policies_exclude = {{access_type::kRead | access_type::kList, {"user4"}}};

    ranger_resource_policy fake_ranger_resource_policy;
    fake_ranger_resource_policy.name = "pegasus_ranger_test";
    fake_ranger_resource_policy.database_names = {"database1", "database2"};
    fake_ranger_resource_policy.table_names = {"database1_table", "database2_table"};
    fake_ranger_resource_policy.policies = fake_policy;

    std::string resource_type_name = enum_to_string(resource_type::kDatabaseTable);
    all_resource_policies fake_all_resource_policies{
        {resource_type_name, {fake_ranger_resource_policy}}};
    // 2.Encode 'fake_all_resource_policies' into a string 'value'
    dsn::blob value =
        json::json_forwarder<all_resource_policies>::encode(fake_all_resource_policies);
    std::string fake_all_resource_policies_str = value.to_string();
    all_resource_policies fake_all_resource_policies_serialized;
    // 3. Decode the string 'value' into 'fake_all_resource_policies_serialized'
    dsn::json::json_forwarder<all_resource_policies>::decode(
        dsn::blob::create_from_bytes(std::move(fake_all_resource_policies_str)),
        fake_all_resource_policies_serialized);

    // 4. Verify the correctness of serialization by checking the data content of
    // 'fake_all_resource_policies' and 'fake_all_resource_policies_serialized'
    EXPECT_EQ(1, fake_all_resource_policies.count(resource_type_name));
    EXPECT_EQ(1, fake_all_resource_policies_serialized.count(resource_type_name));
    ranger_resource_policy policy = fake_all_resource_policies[resource_type_name][0];
    ranger_resource_policy policy_serialized =
        fake_all_resource_policies_serialized[resource_type_name][0];
    ASSERT_EQ(policy.name, policy_serialized.name);
    for (const auto &database_name : policy.database_names) {
        auto it = find(policy_serialized.database_names.begin(),
                       policy_serialized.database_names.end(),
                       database_name);
        ASSERT_NE(it, policy_serialized.database_names.end());
    }
    for (const auto &table_name : policy.table_names) {
        auto it = find(
            policy_serialized.table_names.begin(), policy_serialized.table_names.end(), table_name);
        ASSERT_NE(it, policy_serialized.table_names.end());
    }
    struct test_case
    {
        access_type ac_type;
        std::string user_name;
        bool expected_result;
    } tests[] = {{access_type::kRead, "user", false},      {access_type::kRead, "user1", true},
                 {access_type::kWrite, "user1", true},     {access_type::kCreate, "user1", false},
                 {access_type::kDrop, "user1", false},     {access_type::kList, "user1", true},
                 {access_type::kMetadata, "user1", false}, {access_type::kControl, "user1", false},
                 {access_type::kRead, "user2", true},      {access_type::kWrite, "user2", false},
                 {access_type::kCreate, "user2", false},   {access_type::kDrop, "user2", false},
                 {access_type::kList, "user2", true},      {access_type::kMetadata, "user2", false},
                 {access_type::kControl, "user2", false},  {access_type::kRead, "user3", false},
                 {access_type::kWrite, "user3", false},    {access_type::kCreate, "user3", false},
                 {access_type::kDrop, "user3", false},     {access_type::kList, "user3", true},
                 {access_type::kMetadata, "user3", false}, {access_type::kControl, "user3", false},
                 {access_type::kRead, "user4", true},      {access_type::kWrite, "user4", false},
                 {access_type::kCreate, "user4", false},   {access_type::kDrop, "user4", false},
                 {access_type::kList, "user4", true},      {access_type::kMetadata, "user4", false},
                 {access_type::kControl, "user4", false}};
    for (const auto &test : tests) {
        auto actual_result = policy.policies.allowed(test.ac_type, test.user_name);
        EXPECT_EQ(test.expected_result, actual_result);
        actual_result = policy_serialized.policies.allowed(test.ac_type, test.user_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}

TEST(ranger_resource_policy_manager_test, get_database_name_from_app_name_test)
{
    struct test_case
    {
        std::string app_name;
        std::string expected_result;
    } tests[] = {{"", ""},
                 {".", ""},
                 {"...", ""},
                 {"database_name.", "database_name"},
                 {".table_name", ""},
                 {"app_name", ""},
                 {"database_name.table_name", "database_name"},
                 {"a.b.c", "a"}};
    for (const auto &test : tests) {
        auto actual_result = get_database_name_from_app_name(test.app_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}

TEST(ranger_resource_policy_manager_test, get_table_name_from_app_name_test)
{
    struct test_case
    {
        std::string app_name;
        std::string expected_result;
    } tests[] = {{"", ""},
                 {".", "."},
                 {"...", "..."},
                 {"database_name.", ""},
                 {".table_name", ".table_name"},
                 {"app_name", "app_name"},
                 {"database_name.table_name", "table_name"},
                 {"a.b.c", "b.c"}};
    for (const auto &test : tests) {
        auto actual_result = get_table_name_from_app_name(test.app_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}

class ranger_resource_policy_manager_function_test : public testing::Test
{
public:
    ranger_resource_policy_manager_function_test()
    {
        _manager = new ranger_resource_policy_manager(nullptr);

        acl_policies fake_policy_1;
        fake_policy_1.allow_policies = {
            {access_type::kList | access_type::kMetadata, {"user1", "user2"}}};
        fake_policy_1.allow_policies_exclude = {{access_type::kMetadata, {"user2"}}};
        ranger_resource_policy fake_ranger_resource_policy_1;
        fake_ranger_resource_policy_1.database_names = {"database1"};
        fake_ranger_resource_policy_1.policies = fake_policy_1;
        acl_policies fake_policy_2;
        fake_policy_2.allow_policies = {
            {access_type::kCreate | access_type::kDrop | access_type::kControl,
             {"user3", "user4"}}};
        fake_policy_2.allow_policies_exclude = {{access_type::kControl, {"user4"}}};
        ranger_resource_policy fake_ranger_resource_policy_2;
        fake_ranger_resource_policy_2.database_names = {"database2"};
        fake_ranger_resource_policy_2.policies = fake_policy_2;
        acl_policies fake_policy_3;
        fake_policy_3.allow_policies = {{access_type::kCreate, {"user5", "user6"}}};
        fake_policy_3.allow_policies_exclude = {{access_type::kCreate, {"user6"}}};
        ranger_resource_policy fake_ranger_resource_policy_3;
        fake_ranger_resource_policy_3.database_names = {"*"};
        fake_ranger_resource_policy_3.policies = fake_policy_3;
        _manager->_database_policies_cache = {fake_ranger_resource_policy_1,
                                              fake_ranger_resource_policy_2,
                                              fake_ranger_resource_policy_3};

        acl_policies fake_policy_4;
        fake_policy_4.allow_policies = {{access_type::kMetadata, {"user7", "user8"}}};
        fake_policy_4.allow_policies_exclude = {{access_type::kMetadata, {"user8"}}};
        ranger_resource_policy fake_ranger_resource_policy_4;
        fake_ranger_resource_policy_4.database_names = {"database3"};
        fake_ranger_resource_policy_4.policies = fake_policy_4;
        acl_policies fake_policy_5;
        fake_policy_5.allow_policies = {{access_type::kControl, {"user9", "user10"}}};
        fake_policy_5.allow_policies_exclude = {{access_type::kControl, {"user10"}}};
        ranger_resource_policy fake_ranger_resource_policy_5;
        fake_ranger_resource_policy_5.database_names = {"database4"};
        fake_ranger_resource_policy_5.policies = fake_policy_5;
        _manager->_global_policies_cache = {fake_ranger_resource_policy_4,
                                            fake_ranger_resource_policy_5};
    }

    bool allowed(const int rpc_code, const std::string &user_name, const std::string &database_name)
    {
        return _manager->allowed(rpc_code, user_name, database_name);
    }

private:
    ranger_resource_policy_manager *_manager;
};

TEST_F(ranger_resource_policy_manager_function_test, allowed)
{
    struct test_case
    {
        std::string rpc_code;
        std::string user_name;
        std::string database_name;
        bool expected_result;
    } tests[] = {{"TASK_CODE_INVALID", "user1", "database1", false},
                 {"RPC_CM_CREATE_APP", "user1", "database1", false},
                 {"RPC_CM_CREATE_APP", "user2", "database1", false},
                 {"RPC_CM_LIST_APPS", "user1", "database1", true},
                 {"RPC_CM_LIST_APPS", "user2", "database1", true},
                 {"RPC_CM_GET_MAX_REPLICA_COUNT", "user1", "database1", true},
                 {"RPC_CM_GET_MAX_REPLICA_COUNT", "user2", "database1", false},
                 {"TASK_CODE_INVALID", "user3", "database2", false},
                 {"RPC_CM_CREATE_APP", "user3", "database2", true},
                 {"RPC_CM_CREATE_APP", "user4", "database2", true},
                 {"RPC_CM_START_BACKUP_APP", "user3", "database2", true},
                 {"RPC_CM_START_BACKUP_APP", "user4", "database2", false},
                 {"TASK_CODE_INVALID", "user5", "", false},
                 {"RPC_CM_CREATE_APP", "user5", "", true},
                 {"RPC_CM_CREATE_APP", "user5", "database2", false},
                 {"RPC_CM_CREATE_APP", "user6", "", false},
                 {"RPC_CM_CREATE_APP", "user6", "database2", false},
                 {"TASK_CODE_INVALID", "user7", "database3", false},
                 {"RPC_CM_LIST_NODES", "user7", "database3", true},
                 {"RPC_CM_LIST_NODES", "user8", "database3", false},
                 {"RPC_CM_LIST_APPS", "user7", "database3", true},
                 {"RPC_CM_LIST_APPS", "user8", "database3", false},
                 {"TASK_CODE_INVALID", "user9", "database4", false},
                 {"RPC_CM_LIST_NODES", "user9", "database4", false},
                 {"RPC_CM_LIST_NODES", "user10", "database4", false},
                 {"RPC_CM_LIST_APPS", "user9", "database4", false},
                 {"RPC_CM_LIST_APPS", "user10", "database4", false},
                 {"RPC_CM_CONTROL_META", "user9", "database4", true},
                 {"RPC_CM_CONTROL_META", "user10", "database4", false}};
    for (const auto &test : tests) {
        auto code = task_code::try_get(test.rpc_code, TASK_CODE_INVALID);
        auto actual_result = allowed(code, test.user_name, test.database_name);
        EXPECT_EQ(test.expected_result, actual_result);
    }
}

} // namespace ranger
} // namespace dsn

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

#include <memory>
#include <string>
#include <vector>

#include "common/replication.codes.h"
#include "gtest/gtest.h"
#include "ranger/ranger_resource_policy_manager.h"
#include "rpc/network.h"
#include "rpc/network.sim.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_message.h"
#include "security/access_controller.h"
#include "task/task_code.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/flags.h"

DSN_DECLARE_bool(enable_acl);
DSN_DECLARE_bool(enable_ranger_acl);
DSN_DECLARE_string(super_users);

namespace dsn::security {

struct super_user_case
{
    std::string user_name;
    bool is_super_user;
};

// The super user tests are only for legacy ACL.
class SuperUserTest : public testing::TestWithParam<super_user_case>
{
protected:
    SuperUserTest()
    {
        PRESERVE_FLAG(super_users);

        FLAGS_super_users = "super_user_1, super_user_2";

        // `_meta_access_controller` should be initialized after `FLAGS_super_users`
        // is assigned, since it parses its own super users from `FLAGS_super_users`.
        _meta_access_controller = create_meta_access_controller(nullptr);
    }

    [[nodiscard]] bool is_super_user(const std::string &user_name) const
    {
        return _meta_access_controller->is_super_user(user_name);
    }

private:
    std::shared_ptr<access_controller> _meta_access_controller;
};

TEST_P(SuperUserTest, IsSuperUser)
{
    const auto &test_case = GetParam();
    EXPECT_EQ(test_case.is_super_user, is_super_user(test_case.user_name));
}

const std::vector<super_user_case> super_user_tests = {
    {"", false},
    {"non_super_user", false},
    {"super_user_1", true},
    {"super_user_2", true},
};

INSTANTIATE_TEST_SUITE_P(MetaAccessControllerTest,
                         SuperUserTest,
                         testing::ValuesIn(super_user_tests));

struct rpc_acl_case
{
    bool enable_ranger_acl;
    std::string user_name;
    task_code rpc_code;
    bool is_allowed;
};

// The RPC allow list tests are for both old and Ranger ACLs.
class RpcAclTest : public testing::TestWithParam<rpc_acl_case>
{
protected:
    RpcAclTest()
    {
        PRESERVE_FLAG(enable_acl);
        PRESERVE_FLAG(enable_ranger_acl);
        PRESERVE_FLAG(super_users);

        // Always make legacy ACL enabled, since once Ranger ACL is enabled legacy ACL is also
        // required to be enabled.
        FLAGS_enable_acl = true;

        const auto &test_case = GetParam();
        FLAGS_enable_ranger_acl = test_case.enable_ranger_acl;

        FLAGS_super_users = "super_user_1, super_user_2";

        // `_meta_access_controller` should be initialized after `FLAGS_super_users`
        // is assigned, since it parses its own super users from `FLAGS_super_users`.
        _meta_access_controller = create_meta_access_controller(
            std::make_shared<ranger::ranger_resource_policy_manager>(nullptr));
    }

    bool allowed(dsn::message_ex *msg) const { return _meta_access_controller->allowed(msg); }

private:
    std::shared_ptr<access_controller> _meta_access_controller;
};

TEST_P(RpcAclTest, RpcAllowed)
{
    PRESERVE_FLAG(enable_acl);
    PRESERVE_FLAG(enable_ranger_acl);

    // Always make legacy ACL enabled, since once Ranger ACL is enabled legacy ACL is also
    // required to be enabled.
    FLAGS_enable_acl = true;

    const auto &test_case = GetParam();
    FLAGS_enable_ranger_acl = test_case.enable_ranger_acl;

    const std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(nullptr, nullptr));
    const auto sim_session =
        sim_net->create_client_session(rpc_address::from_host_port("localhost", 10086));

    // Specify the client user.
    sim_session->set_client_username(test_case.user_name);

    const dsn::message_ptr msg = message_ex::create_request(test_case.rpc_code);
    msg->io_session = sim_session;

    ASSERT_EQ(test_case.is_allowed, allowed(msg));
}

const std::vector<rpc_acl_case> rpc_acl_tests = {
    // Whether an RPC request is allowed depends on the allow list for legacy ACL once Ranger
    // ACL is disabled and the client user is a non-super user.
    {false, "non_super_user", RPC_CM_CLUSTER_INFO, true},
    {false, "non_super_user", RPC_CM_LIST_APPS, true},
    {false, "non_super_user", RPC_CM_DDD_DIAGNOSE, true},
    {false, "non_super_user", RPC_CM_LIST_NODES, true},
    {false, "non_super_user", RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, true},
    {false, "non_super_user", RPC_CM_START_RECOVERY, false},

    // Any RPC request will be allowed for legacy ACL once Ranger ACL is disabled and the
    // client user is a super user.
    {false, "super_user_1", RPC_CM_CLUSTER_INFO, true},
    {false, "super_user_2", RPC_CM_LIST_APPS, true},
    {false, "super_user_1", RPC_CM_DDD_DIAGNOSE, true},
    {false, "super_user_2", RPC_CM_LIST_NODES, true},
    {false, "super_user_1", RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, true},
    {false, "super_user_2", RPC_CM_START_RECOVERY, true},

    // Whether an RPC request is allowed depends on the allow list for Ranger ACL once it
    // is enabled.
    {true, "non_super_user", RPC_CM_CLUSTER_INFO, false},
    {true, "non_super_user", RPC_CM_LIST_APPS, false},
    {true, "non_super_user", RPC_CM_DDD_DIAGNOSE, false},
    {true, "non_super_user", RPC_CM_LIST_NODES, false},
    {true, "non_super_user", RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, true},
    {true, "non_super_user", RPC_CM_START_RECOVERY, false},

    // Whether an RPC request is allowed depends on the allow list for Ranger ACL once it
    // is enabled even if the client user is a super user for legacy ACL.
    {true, "super_user_1", RPC_CM_CLUSTER_INFO, false},
    {true, "super_user_2", RPC_CM_LIST_APPS, false},
    {true, "super_user_1", RPC_CM_DDD_DIAGNOSE, false},
    {true, "super_user_2", RPC_CM_LIST_NODES, false},
    {true, "super_user_1", RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, true},
    {true, "super_user_2", RPC_CM_START_RECOVERY, false},
};

INSTANTIATE_TEST_SUITE_P(MetaAccessControllerTest, RpcAclTest, testing::ValuesIn(rpc_acl_tests));

} // namespace dsn::security

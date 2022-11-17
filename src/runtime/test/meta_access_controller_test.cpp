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
#include "utils/flags.h"
#include "runtime/rpc/network.sim.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"
#include "runtime/security/access_controller.h"

namespace dsn {
namespace security {
DSN_DECLARE_bool(enable_acl);

class meta_access_controller_test : public testing::Test
{
public:
    meta_access_controller_test() { _meta_access_controller = create_meta_access_controller(); }

    void set_super_user(const std::string &super_user)
    {
        _meta_access_controller->_super_users.insert(super_user);
    }

    bool pre_check(const std::string &user_name)
    {
        return _meta_access_controller->pre_check(user_name);
    }

    bool allowed(dsn::message_ex *msg) { return _meta_access_controller->allowed(msg); }

    std::unique_ptr<access_controller> _meta_access_controller;
};

TEST_F(meta_access_controller_test, pre_check)
{
    const std::string SUPER_USER_NAME = "super_user";
    struct
    {
        bool enable_acl;
        std::string user_name;
        bool result;
    } tests[] = {{true, "not_super_user", false},
                 {false, "not_super_user", true},
                 {true, SUPER_USER_NAME, true}};

    bool origin_enable_acl = FLAGS_enable_acl;
    set_super_user(SUPER_USER_NAME);

    for (const auto &test : tests) {
        FLAGS_enable_acl = test.enable_acl;
        ASSERT_EQ(pre_check(test.user_name), test.result);
    }

    FLAGS_enable_acl = origin_enable_acl;
}

TEST_F(meta_access_controller_test, allowed)
{
    struct
    {
        task_code rpc_code;
        bool result;
    } tests[] = {{RPC_CM_LIST_APPS, true},
                 {RPC_CM_LIST_NODES, true},
                 {RPC_CM_CLUSTER_INFO, true},
                 {RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, true},
                 {RPC_CM_START_RECOVERY, false}};

    bool origin_enable_acl = FLAGS_enable_acl;
    FLAGS_enable_acl = true;

    std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(nullptr, nullptr));
    auto sim_session = sim_net->create_client_session(rpc_address("localhost", 10086));
    for (const auto &test : tests) {
        dsn::message_ptr msg = message_ex::create_request(test.rpc_code);
        msg->io_session = sim_session;

        ASSERT_EQ(allowed(msg), test.result);
    }

    FLAGS_enable_acl = origin_enable_acl;
}
} // namespace security
} // namespace dsn

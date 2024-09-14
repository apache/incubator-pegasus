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
#include <unordered_set>
#include <utility>

#include "common/replication.codes.h"
#include "gtest/gtest.h"
#include "ranger/access_type.h"
#include "rpc/network.h"
#include "rpc/network.sim.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_message.h"
#include "security/replica_access_controller.h"
#include "utils/autoref_ptr.h"
#include "utils/flags.h"

DSN_DECLARE_bool(enable_acl);

namespace dsn {
namespace security {

class replica_access_controller_test : public testing::Test
{
public:
    replica_access_controller_test()
    {
        _replica_access_controller = std::make_unique<replica_access_controller>("test");
    }

    bool allowed(dsn::message_ex *msg)
    {
        return _replica_access_controller->allowed(msg, dsn::ranger::access_type::kRead);
    }

    void set_replica_users(std::unordered_set<std::string> &&replica_users)
    {
        _replica_access_controller->_allowed_users.swap(replica_users);
    }

    std::unique_ptr<replica_access_controller> _replica_access_controller;
};

TEST_F(replica_access_controller_test, allowed)
{
    struct
    {
        std::unordered_set<std::string> replica_users;
        std::string client_user;
        bool result;
    } tests[] = {{{"replica_user1", "replica_user2"}, "replica_user1", true},
                 {{"replica_user1", "replica_user2"}, "not_replica_user", false},
                 {{}, "user_name", true}};

    bool origin_enable_acl = FLAGS_enable_acl;
    FLAGS_enable_acl = true;

    std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(nullptr, nullptr));
    auto sim_session =
        sim_net->create_client_session(rpc_address::from_host_port("localhost", 10086));
    dsn::message_ptr msg = message_ex::create_request(RPC_CM_LIST_APPS);
    msg->io_session = sim_session;

    for (auto &test : tests) {
        set_replica_users(std::move(test.replica_users));
        sim_session->set_client_username(test.client_user);

        ASSERT_EQ(allowed(msg), test.result);
    }

    FLAGS_enable_acl = origin_enable_acl;
}
} // namespace security
} // namespace dsn

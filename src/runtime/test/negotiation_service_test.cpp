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

#include "runtime/security/negotiation_service.h"
#include "runtime/security/negotiation_utils.h"
#include "runtime/rpc/network.sim.h"

#include <gtest/gtest.h>
#include <dsn/utility/flags.h>
#include <dsn/dist/failure_detector/fd.code.definition.h>
#include <http/http_server_impl.h>

namespace dsn {
namespace security {
DSN_DECLARE_bool(enable_auth);

class negotiation_service_test : public testing::Test
{
public:
    negotiation_rpc create_fake_rpc()
    {
        std::unique_ptr<tools::sim_network_provider> sim_net(
            new tools::sim_network_provider(nullptr, nullptr));
        auto sim_session =
            sim_net->create_server_session(rpc_address("localhost", 10086), rpc_session_ptr());
        auto rpc = negotiation_rpc(make_unique<negotiation_request>(), RPC_NEGOTIATION);
        rpc.dsn_request()->io_session = sim_session;
        return rpc;
    }

    rpc_session_ptr create_fake_session()
    {
        std::unique_ptr<tools::sim_network_provider> sim_net(
            new tools::sim_network_provider(nullptr, nullptr));
        return sim_net->create_server_session(rpc_address("localhost", 10086), rpc_session_ptr());
    }

    void on_negotiation_request(negotiation_rpc rpc)
    {
        negotiation_service::instance().on_negotiation_request(rpc);
    }

    bool on_rpc_recv_msg(message_ex *msg)
    {
        return negotiation_service::instance().on_rpc_recv_msg(msg);
    }
};

TEST_F(negotiation_service_test, disable_auth)
{
    RPC_MOCKING(negotiation_rpc)
    {
        FLAGS_enable_auth = false;
        auto rpc = create_fake_rpc();
        on_negotiation_request(rpc);

        ASSERT_EQ(rpc.response().status, negotiation_status::type::SASL_AUTH_DISABLE);
    }
}

TEST_F(negotiation_service_test, on_rpc_recv_msg)
{
    struct
    {
        task_code rpc_code;
        bool negotiation_succeed;
        bool return_value;
    } tests[] = {{RPC_NEGOTIATION, true, true},
                 {RPC_NEGOTIATION_ACK, true, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING, true, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING_ACK, true, true},
                 {RPC_NEGOTIATION, false, true},
                 {RPC_HTTP_SERVICE, true, true},
                 {RPC_HTTP_SERVICE, false, false}};

    for (const auto &test : tests) {
        message_ptr msg = dsn::message_ex::create_request(test.rpc_code, 0, 0);
        auto sim_session = create_fake_session();
        msg->io_session = sim_session;
        if (test.negotiation_succeed) {
            sim_session->set_negotiation_succeed();
        }

        ASSERT_EQ(test.return_value, on_rpc_recv_msg(msg));
    }
}
} // namespace security
} // namespace dsn

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

#include "runtime/security/negotiation_manager.h"

#include "failure_detector/fd.code.definition.h"
#include "gtest/gtest.h"
#include "http/http_server.h"
#include "nfs/nfs_code_definition.h"
#include "runtime/rpc/network.h"
#include "runtime/rpc/network.sim.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/security/negotiation_utils.h"
#include "runtime/task/task_code.h"
#include "security_types.h"
#include "utils/autoref_ptr.h"
#include "utils/flags.h"

namespace dsn {
namespace security {
DSN_DECLARE_bool(enable_auth);
DSN_DECLARE_bool(mandatory_auth);

class negotiation_manager_test : public testing::Test
{
public:
    negotiation_rpc create_fake_rpc()
    {
        std::unique_ptr<tools::sim_network_provider> sim_net(
            new tools::sim_network_provider(nullptr, nullptr));
        auto sim_session =
            sim_net->create_server_session(rpc_address("localhost", 10086), rpc_session_ptr());
        auto rpc = negotiation_rpc(std::make_unique<negotiation_request>(), RPC_NEGOTIATION);
        rpc.dsn_request()->io_session = sim_session;
        return rpc;
    }

    rpc_session_ptr create_fake_session(bool is_client)
    {
        std::unique_ptr<tools::sim_network_provider> sim_net(
            new tools::sim_network_provider(nullptr, nullptr));
        if (is_client) {
            return sim_net->create_client_session(rpc_address("localhost", 10086));
        } else {
            return sim_net->create_server_session(rpc_address("localhost", 10086),
                                                  rpc_session_ptr());
        }
    }

    void on_negotiation_request(negotiation_rpc rpc)
    {
        negotiation_manager::instance().on_negotiation_request(rpc);
    }

    bool on_rpc_recv_msg(message_ex *msg)
    {
        return negotiation_manager::instance().on_rpc_recv_msg(msg);
    }

    bool on_rpc_send_msg(message_ex *msg)
    {
        return negotiation_manager::instance().on_rpc_send_msg(msg);
    }
};

TEST_F(negotiation_manager_test, disable_auth)
{
    RPC_MOCKING(negotiation_rpc)
    {
        FLAGS_enable_auth = false;
        auto rpc = create_fake_rpc();
        on_negotiation_request(rpc);

        ASSERT_EQ(rpc.response().status, negotiation_status::type::SASL_AUTH_DISABLE);
    }
}

TEST_F(negotiation_manager_test, on_rpc_recv_msg)
{
    struct
    {
        task_code rpc_code;
        bool negotiation_succeed;
        bool mandatory_auth;
        bool is_client;
        bool return_value;
    } tests[] = {{RPC_NEGOTIATION, false, true, false, true},
                 {RPC_NEGOTIATION, false, true, true, true},
                 {RPC_NEGOTIATION_ACK, false, true, false, true},
                 {RPC_NEGOTIATION_ACK, false, true, true, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING, false, true, false, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING, false, true, true, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING_ACK, false, true, false, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING_ACK, false, true, true, true},
                 {RPC_HTTP_SERVICE, false, true, false, true},
                 {RPC_HTTP_SERVICE, false, true, true, true},
                 {RPC_HTTP_SERVICE_ACK, false, true, false, true},
                 {RPC_HTTP_SERVICE_ACK, false, true, true, true},
                 {service::RPC_NFS_COPY, true, true, false, true},
                 {service::RPC_NFS_COPY, true, true, true, true},
                 {service::RPC_NFS_COPY, false, false, false, true},
                 {service::RPC_NFS_COPY, false, false, true, false},
                 {service::RPC_NFS_COPY, false, true, true, false},
                 {service::RPC_NFS_COPY, false, true, false, false}};

    for (const auto &test : tests) {
        FLAGS_mandatory_auth = test.mandatory_auth;
        message_ptr msg = dsn::message_ex::create_request(test.rpc_code, 0, 0);
        auto sim_session = create_fake_session(test.is_client);
        msg->io_session = sim_session;
        if (test.negotiation_succeed) {
            sim_session->set_negotiation_succeed();
        }

        ASSERT_EQ(test.return_value, on_rpc_recv_msg(msg));
    }
}

TEST_F(negotiation_manager_test, on_rpc_send_msg)
{
    struct
    {
        task_code rpc_code;
        bool negotiation_succeed;
        bool mandatory_auth;
        bool is_client;
        bool return_value;
    } tests[] = {{RPC_NEGOTIATION, false, true, false, true},
                 {RPC_NEGOTIATION, false, true, true, true},
                 {RPC_NEGOTIATION_ACK, false, true, false, true},
                 {RPC_NEGOTIATION_ACK, false, true, true, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING, false, true, false, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING, false, true, true, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING_ACK, false, true, false, true},
                 {fd::RPC_FD_FAILURE_DETECTOR_PING_ACK, false, true, true, true},
                 {RPC_HTTP_SERVICE, false, true, false, true},
                 {RPC_HTTP_SERVICE, false, true, true, true},
                 {RPC_HTTP_SERVICE_ACK, false, true, false, true},
                 {RPC_HTTP_SERVICE_ACK, false, true, true, true},
                 {service::RPC_NFS_COPY, true, true, false, true},
                 {service::RPC_NFS_COPY, true, true, true, true},
                 {service::RPC_NFS_COPY, false, false, false, true},
                 {service::RPC_NFS_COPY, false, false, true, false},
                 {service::RPC_NFS_COPY, false, true, true, false},
                 {service::RPC_NFS_COPY, false, true, false, false}};

    for (const auto &test : tests) {
        FLAGS_mandatory_auth = test.mandatory_auth;
        message_ptr msg = dsn::message_ex::create_request(test.rpc_code, 0, 0);
        auto sim_session = create_fake_session(test.is_client);
        msg->io_session = sim_session;
        if (test.negotiation_succeed) {
            sim_session->set_negotiation_succeed();
        }

        ASSERT_EQ(test.return_value, on_rpc_send_msg(msg));
    }
}
} // namespace security
} // namespace dsn

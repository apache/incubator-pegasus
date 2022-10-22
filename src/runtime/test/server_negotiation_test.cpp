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

#include "runtime/security/server_negotiation.h"
#include "runtime/security/negotiation_utils.h"
#include "runtime/rpc/network.sim.h"

#include <gtest/gtest.h>
#include "utils/fail_point.h"

namespace dsn {
namespace security {
class server_negotiation_test : public testing::Test
{
public:
    server_negotiation_test()
    {
        std::unique_ptr<tools::sim_network_provider> sim_net(
            new tools::sim_network_provider(nullptr, nullptr));
        _sim_session =
            sim_net->create_server_session(rpc_address("localhost", 10086), rpc_session_ptr());
        _srv_negotiation = make_unique<server_negotiation>(_sim_session);
    }

    negotiation_rpc create_negotiation_rpc(negotiation_status::type status, const std::string &msg)
    {
        auto request = make_unique<negotiation_request>();
        request->status = status;
        request->msg = dsn::blob::create_from_bytes(msg.data(), msg.length());
        return negotiation_rpc(std::move(request), RPC_NEGOTIATION);
    }

    void on_list_mechanisms(negotiation_rpc rpc) { _srv_negotiation->on_list_mechanisms(rpc); }

    void on_select_mechanism(negotiation_rpc rpc) { _srv_negotiation->on_select_mechanism(rpc); }

    void on_initiate(negotiation_rpc rpc) { _srv_negotiation->on_initiate(rpc); }

    void on_challenge_resp(negotiation_rpc rpc) { _srv_negotiation->on_challenge_resp(rpc); }

    negotiation_status::type get_negotiation_status() { return _srv_negotiation->_status; }

    // _sim_session is used for holding the sim_rpc_session which is created in ctor,
    // in case it is released. Because negotiation keeps only a raw pointer.
    rpc_session_ptr _sim_session;
    std::unique_ptr<server_negotiation> _srv_negotiation;
};

TEST_F(server_negotiation_test, on_list_mechanisms)
{
    struct
    {
        negotiation_status::type req_status;
        negotiation_status::type resp_status;
        std::string resp_msg;
        negotiation_status::type nego_status;
    } tests[] = {{negotiation_status::type::SASL_LIST_MECHANISMS,
                  negotiation_status::type::SASL_LIST_MECHANISMS_RESP,
                  "GSSAPI",
                  negotiation_status::type::SASL_LIST_MECHANISMS_RESP},
                 {negotiation_status::type::SASL_SELECT_MECHANISMS,
                  negotiation_status::type::INVALID,
                  "",
                  negotiation_status::type::SASL_AUTH_FAIL}};

    RPC_MOCKING(negotiation_rpc)
    {
        for (const auto &test : tests) {
            auto rpc = create_negotiation_rpc(test.req_status, "");
            on_list_mechanisms(rpc);

            ASSERT_EQ(rpc.response().status, test.resp_status);
            ASSERT_EQ(rpc.response().msg.to_string(), test.resp_msg);
            ASSERT_EQ(get_negotiation_status(), test.nego_status);
        }
    }
}

TEST_F(server_negotiation_test, on_select_mechanism)
{
    struct
    {
        std::string sasl_init_result;
        negotiation_status::type req_status;
        std::string req_msg;
        negotiation_status::type resp_status;
        negotiation_status::type nego_status;
    } tests[] = {{
                     "ERR_OK",
                     negotiation_status::type::SASL_SELECT_MECHANISMS,
                     "GSSAPI",
                     negotiation_status::type::SASL_SELECT_MECHANISMS_RESP,
                     negotiation_status::type::SASL_SELECT_MECHANISMS_RESP,
                 },
                 {"ERR_OK",
                  negotiation_status::type::SASL_SELECT_MECHANISMS,
                  "TEST",
                  negotiation_status::type::INVALID,
                  negotiation_status::type::SASL_AUTH_FAIL},
                 {"ERR_TIMEOUT",
                  negotiation_status::type::SASL_SELECT_MECHANISMS,
                  "GSSAPI",
                  negotiation_status::type::INVALID,
                  negotiation_status::type::SASL_AUTH_FAIL},
                 {"ERR_OK",
                  negotiation_status::type::SASL_INITIATE,
                  "GSSAPI",
                  negotiation_status::type::INVALID,
                  negotiation_status::type::SASL_AUTH_FAIL}};

    RPC_MOCKING(negotiation_rpc)
    {
        for (const auto &test : tests) {
            fail::setup();
            fail::cfg("sasl_server_wrapper_init", "return(" + test.sasl_init_result + ")");

            auto rpc = create_negotiation_rpc(test.req_status, test.req_msg);
            on_select_mechanism(rpc);
            ASSERT_EQ(rpc.response().status, test.resp_status);
            ASSERT_EQ(get_negotiation_status(), test.nego_status);

            fail::teardown();
        }
    }
}

TEST_F(server_negotiation_test, on_initiate)
{
    struct
    {
        std::string sasl_start_result;
        std::string sasl_retrieve_username_result;
        negotiation_status::type req_status;
        negotiation_status::type resp_status;
        negotiation_status::type nego_status;
    } tests[] = {
        {"ERR_TIMEOUT",
         "ERR_OK",
         negotiation_status::type::SASL_INITIATE,
         negotiation_status::type::INVALID,
         negotiation_status::type::SASL_AUTH_FAIL},
        {"ERR_OK",
         "ERR_OK",
         negotiation_status::type::SASL_SELECT_MECHANISMS,
         negotiation_status::type::INVALID,
         negotiation_status::type::SASL_AUTH_FAIL},
        {"ERR_OK",
         "ERR_TIMEOUT",
         negotiation_status::type::SASL_INITIATE,
         negotiation_status::type::INVALID,
         negotiation_status::type::SASL_AUTH_FAIL},
        {"ERR_SASL_INCOMPLETE",
         "ERR_OK",
         negotiation_status::type::SASL_INITIATE,
         negotiation_status::type::SASL_CHALLENGE,
         negotiation_status::type::SASL_CHALLENGE},
        {"ERR_OK",
         "ERR_OK",
         negotiation_status::type::SASL_INITIATE,
         negotiation_status::type::SASL_SUCC,
         negotiation_status::type::SASL_SUCC},
    };

    RPC_MOCKING(negotiation_rpc)
    {
        for (const auto &test : tests) {
            fail::setup();
            fail::cfg("sasl_server_wrapper_start", "return(" + test.sasl_start_result + ")");
            fail::cfg("sasl_wrapper_retrieve_username",
                      "return(" + test.sasl_retrieve_username_result + ")");

            auto rpc = create_negotiation_rpc(test.req_status, "");
            on_initiate(rpc);
            ASSERT_EQ(rpc.response().status, test.resp_status);
            ASSERT_EQ(get_negotiation_status(), test.nego_status);

            fail::teardown();
        }
    }
}

TEST_F(server_negotiation_test, on_challenge_resp)
{
    struct
    {
        std::string sasl_step_result;
        std::string sasl_retrieve_username_result;
        negotiation_status::type req_status;
        negotiation_status::type resp_status;
        negotiation_status::type nego_status;
    } tests[] = {{"ERR_TIMEOUT",
                  "ERR_OK",
                  negotiation_status::type::SASL_CHALLENGE_RESP,
                  negotiation_status::type::INVALID,
                  negotiation_status::type::SASL_AUTH_FAIL},
                 {"ERR_OK",
                  "ERR_OK",
                  negotiation_status::type::SASL_SELECT_MECHANISMS,
                  negotiation_status::type::INVALID,
                  negotiation_status::type::SASL_AUTH_FAIL},
                 {"ERR_OK",
                  "ERR_TIMEOUT",
                  negotiation_status::type::SASL_CHALLENGE_RESP,
                  negotiation_status::type::INVALID,
                  negotiation_status::type::SASL_AUTH_FAIL},
                 {"ERR_SASL_INCOMPLETE",
                  "ERR_OK",
                  negotiation_status::type::SASL_CHALLENGE_RESP,
                  negotiation_status::type::SASL_CHALLENGE,
                  negotiation_status::type::SASL_CHALLENGE},
                 {"ERR_OK",
                  "ERR_OK",
                  negotiation_status::type::SASL_CHALLENGE_RESP,
                  negotiation_status::type::SASL_SUCC,
                  negotiation_status::type::SASL_SUCC}};

    RPC_MOCKING(negotiation_rpc)
    {
        for (const auto &test : tests) {
            fail::setup();
            fail::cfg("sasl_server_wrapper_step", "return(" + test.sasl_step_result + ")");
            fail::cfg("sasl_wrapper_retrieve_username",
                      "return(" + test.sasl_retrieve_username_result + ")");

            auto rpc = create_negotiation_rpc(test.req_status, "");
            on_challenge_resp(rpc);
            ASSERT_EQ(rpc.response().status, test.resp_status);
            ASSERT_EQ(get_negotiation_status(), test.nego_status);

            fail::teardown();
        }
    }
}
} // namespace security
} // namespace dsn

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
#include <utility>

#include "gtest/gtest.h"
#include "rpc/network.sim.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_message.h"
#include "security/client_negotiation.h"
#include "security/negotiation.h"
#include "security_types.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"

namespace dsn {
namespace security {
class client_negotiation_test : public testing::Test
{
public:
    client_negotiation_test()
    {
        std::unique_ptr<tools::sim_network_provider> sim_net(
            new tools::sim_network_provider(nullptr, nullptr));
        _sim_session =
            sim_net->create_client_session(rpc_address::from_host_port("localhost", 10086));
        _client_negotiation = std::make_unique<client_negotiation>(_sim_session);
    }

    void on_recv_mechanism(const negotiation_response &resp)
    {
        _client_negotiation->on_recv_mechanisms(resp);
    }

    void handle_response(error_code err, const negotiation_response &resp)
    {
        _client_negotiation->handle_response(err, std::move(resp));
    }

    void on_mechanism_selected(const negotiation_response &resp)
    {
        _client_negotiation->on_mechanism_selected(resp);
    }

    void on_challenge(const negotiation_response &resp) { _client_negotiation->on_challenge(resp); }

    const std::string &get_selected_mechanism() { return _client_negotiation->_selected_mechanism; }

    negotiation_status::type get_negotiation_status() { return _client_negotiation->_status; }

    // _sim_session is used for holding the sim_rpc_session which is created in ctor,
    // in case it is released. Because negotiation keeps only a raw pointer.
    rpc_session_ptr _sim_session;
    std::unique_ptr<client_negotiation> _client_negotiation;
};

TEST_F(client_negotiation_test, on_recv_mechanisms)
{
    struct
    {
        negotiation_status::type resp_status;
        std::string resp_msg;
        std::string selected_mechanism;
    } tests[] = {{negotiation_status::type::SASL_SELECT_MECHANISMS, "GSSAPI", ""},
                 {negotiation_status::type::SASL_LIST_MECHANISMS_RESP, "TEST1", ""},
                 {negotiation_status::type::SASL_LIST_MECHANISMS_RESP, "TEST1, TEST2", ""},
                 {negotiation_status::type::SASL_LIST_MECHANISMS_RESP, "TEST1, GSSAPI", "GSSAPI"},
                 {negotiation_status::type::SASL_LIST_MECHANISMS_RESP, "GSSAPI", "GSSAPI"}};

    RPC_MOCKING(negotiation_rpc)
    {
        for (const auto &test : tests) {
            negotiation_response resp;
            resp.status = test.resp_status;
            resp.msg = blob::create_from_bytes(test.resp_msg.data(), test.resp_msg.length());
            on_recv_mechanism(resp);

            ASSERT_EQ(get_selected_mechanism(), test.selected_mechanism);
        }
    }
}

TEST_F(client_negotiation_test, handle_response)
{
    struct
    {
        error_code resp_err;
        negotiation_status::type resp_status;
        negotiation_status::type neg_status;
    } tests[] = {
        {ERR_TIMEOUT,
         negotiation_status::type::SASL_SELECT_MECHANISMS,
         negotiation_status::type::SASL_AUTH_FAIL},
        {ERR_OK, negotiation_status::type::SASL_AUTH_DISABLE, negotiation_status::type::SASL_SUCC}};

    for (const auto &test : tests) {
        negotiation_response resp;
        resp.status = test.resp_status;
        handle_response(test.resp_err, resp);

        ASSERT_EQ(get_negotiation_status(), test.neg_status);
    }
}

TEST_F(client_negotiation_test, on_mechanism_selected)
{
    struct
    {
        std::string sasl_init_result;
        std::string sasl_start_result;
        negotiation_status::type resp_status;
        negotiation_status::type neg_status;
    } tests[] = {{"ERR_OK",
                  "ERR_OK",
                  negotiation_status::type::SASL_SELECT_MECHANISMS_RESP,
                  negotiation_status::type::SASL_INITIATE},
                 {"ERR_OK",
                  "ERR_SASL_INCOMPLETE",
                  negotiation_status::type::SASL_SELECT_MECHANISMS_RESP,
                  negotiation_status::type::SASL_INITIATE},
                 {"ERR_OK",
                  "ERR_TIMEOUT",
                  negotiation_status::type::SASL_SELECT_MECHANISMS_RESP,
                  negotiation_status::type::SASL_AUTH_FAIL},
                 {"ERR_TIMEOUT",
                  "ERR_OK",
                  negotiation_status::type::SASL_SELECT_MECHANISMS_RESP,
                  negotiation_status::type::SASL_AUTH_FAIL},
                 {"ERR_OK",
                  "ERR_OK",
                  negotiation_status::type::SASL_SELECT_MECHANISMS,
                  negotiation_status::type::SASL_AUTH_FAIL}};

    RPC_MOCKING(negotiation_rpc)
    {
        for (const auto &test : tests) {
            fail::setup();
            fail::cfg("sasl_client_wrapper_init", "return(" + test.sasl_init_result + ")");
            fail::cfg("sasl_client_wrapper_start", "return(" + test.sasl_start_result + ")");

            negotiation_response resp;
            resp.status = test.resp_status;
            on_mechanism_selected(resp);
            ASSERT_EQ(get_negotiation_status(), test.neg_status);

            fail::teardown();
        }
    }
}

TEST_F(client_negotiation_test, on_challenge)
{
    struct
    {
        std::string sasl_step_result;
        negotiation_status::type resp_status;
        negotiation_status::type neg_status;
    } tests[] = {
        {"ERR_OK",
         negotiation_status::type::SASL_CHALLENGE,
         negotiation_status::type::SASL_CHALLENGE_RESP},
        {"ERR_SASL_INCOMPLETE",
         negotiation_status::type::SASL_CHALLENGE,
         negotiation_status::type::SASL_CHALLENGE_RESP},
        {"ERR_TIMEOUT",
         negotiation_status::type::SASL_CHALLENGE,
         negotiation_status::type::SASL_AUTH_FAIL},
        {"ERR_OK", negotiation_status::type::SASL_SUCC, negotiation_status::type::SASL_SUCC}};

    RPC_MOCKING(negotiation_rpc)
    {
        for (const auto &test : tests) {
            fail::setup();
            fail::cfg("sasl_client_wrapper_step", "return(" + test.sasl_step_result + ")");

            negotiation_response resp;
            resp.status = test.resp_status;
            on_challenge(resp);
            ASSERT_EQ(get_negotiation_status(), test.neg_status);

            fail::teardown();
        }
    }
}
} // namespace security
} // namespace dsn

/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <string.h>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/global_config.h"
#include "rpc/asio_net_provider.h"
#include "rpc/network.h"
#include "rpc/network.sim.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_engine.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "runtime/service_engine.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "runtime/test_utils.h"
#include "utils/autoref_ptr.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/synchronize.h"

DSN_DECLARE_uint32(conn_threshold_per_ip);

namespace dsn {

namespace {

class asio_network_provider_test : public tools::asio_network_provider
{
public:
    asio_network_provider_test(rpc_engine *srv, network *inner_provider)
        : tools::asio_network_provider(srv, inner_provider)
    {
    }
};

DEFINE_TASK_CODE_RPC(RPC_TEST_NETPROVIDER, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)

void rpc_server_response(dsn::message_ex *request)
{
    std::string str_command;
    ::dsn::unmarshall(request, str_command);
    dsn::message_ex *response = request->create_response();
    ::dsn::marshall(response, str_command);
    dsn_rpc_reply(response);
}

} // anonymous namespace

class NetProviderTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        if (dsn::service_engine::instance().spec().semaphore_factory_name ==
            "dsn::tools::sim_semaphore_provider") {
            GTEST_SKIP() << "Skip the test in simulator mode, set 'tool = nativerun' "
                            "in '[core]' section in config file to enable it.";
        }

        ASSERT_TRUE(dsn_rpc_register_handler(
            RPC_TEST_NETPROVIDER, "rpc.test.netprovider", rpc_server_response));
    }

    void TearDown() override
    {
        ASSERT_TRUE(dsn_rpc_unregiser_handler(RPC_TEST_NETPROVIDER));

        ++_test_port;
    }

    void response_handler(bool reject,
                          const std::string &expected_content,
                          dsn::error_code ec,
                          dsn::message_ex *req,
                          dsn::message_ex *resp)
    {
        const auto on_completed = defer([this]() { _response_completed.notify(); });

        if (reject) {
            ASSERT_EQ(ERR_TIMEOUT, ec);
            return;
        }

        if (ec != ERR_OK) {
            LOG_INFO("error msg: {}", ec);
            return;
        }

        std::string actual_content;
        ::dsn::unmarshall(resp, actual_content);
        ASSERT_EQ(expected_content, actual_content);
    }

    void rpc_client_session_send(rpc_session_ptr client_session, bool reject)
    {
        message_ex *msg = message_ex::create_request(RPC_TEST_NETPROVIDER, 0, 0);

        const std::string expected_content("hello world");
        ::dsn::marshall(msg, expected_content);

        rpc_response_task_ptr t(new rpc_response_task(
            msg,
            [reject, expected_content, this](
                dsn::error_code ec, dsn::message_ex *req, dsn::message_ex *resp) {
                response_handler(reject, expected_content, ec, req, resp);
            },
            0));
        client_session->net().engine()->matcher()->on_call(msg, t);

        client_session->send_message(msg);
        wait_response();
    }

    void wait_response() { _response_completed.wait(); }

    static int _test_port;
    utils::notify_event _response_completed;
};

int NetProviderTest::_test_port = 20401;

TEST_F(NetProviderTest, AsioNetProvider)
{
    std::unique_ptr<tools::asio_network_provider> asio_network(
        new tools::asio_network_provider(task::get_current_rpc(), nullptr));

    error_code start_result;
    start_result = asio_network->start(RPC_CHANNEL_TCP, _test_port, true);
    ASSERT_TRUE(start_result == ERR_OK);

    // the same asio network handle, start only client is ok
    start_result = asio_network->start(RPC_CHANNEL_TCP, _test_port, true);
    ASSERT_TRUE(start_result == ERR_OK);

    rpc_address network_addr = asio_network->address();
    ASSERT_TRUE(network_addr.port() == _test_port);

    std::unique_ptr<tools::asio_network_provider> asio_network2(
        new tools::asio_network_provider(task::get_current_rpc(), nullptr));
    start_result = asio_network2->start(RPC_CHANNEL_TCP, _test_port, true);
    ASSERT_TRUE(start_result == ERR_OK);

    start_result = asio_network2->start(RPC_CHANNEL_TCP, _test_port, false);
    ASSERT_TRUE(start_result == ERR_OK);
    LOG_INFO("result: {}", start_result);

    start_result = asio_network2->start(RPC_CHANNEL_TCP, _test_port, false);
    ASSERT_TRUE(start_result == ERR_SERVICE_ALREADY_RUNNING);
    LOG_INFO("result: {}", start_result);

    rpc_session_ptr client_session =
        asio_network->create_client_session(rpc_address::from_host_port("localhost", _test_port));
    client_session->connect();

    rpc_client_session_send(client_session, false);
}

TEST_F(NetProviderTest, AsioUdpProvider)
{
    std::unique_ptr<tools::asio_udp_provider> client(
        new tools::asio_udp_provider(task::get_current_rpc(), nullptr));

    error_code start_result;
    start_result = client->start(RPC_CHANNEL_UDP, 0, true);
    ASSERT_TRUE(start_result == ERR_OK);

    start_result = client->start(RPC_CHANNEL_UDP, _test_port, false);
    ASSERT_TRUE(start_result == ERR_OK);

    message_ex *msg = message_ex::create_request(RPC_TEST_NETPROVIDER, 0, 0);

    const std::string expected_content("hello world");
    ::dsn::marshall(msg, expected_content);

    rpc_response_task_ptr t(new rpc_response_task(
        msg,
        [expected_content, this](dsn::error_code ec, dsn::message_ex *req, dsn::message_ex *resp) {
            response_handler(false, expected_content, ec, req, resp);
        },
        0));
    client->engine()->matcher()->on_call(msg, t);

    client->send_message(msg);
    wait_response();
}

TEST_F(NetProviderTest, SimNetProvider)
{
    std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(task::get_current_rpc(), nullptr));

    error_code ans;
    ans = sim_net->start(RPC_CHANNEL_TCP, _test_port, false);
    ASSERT_TRUE(ans == ERR_OK);

    ans = sim_net->start(RPC_CHANNEL_TCP, _test_port, false);
    ASSERT_TRUE(ans == ERR_ADDRESS_ALREADY_USED);

    rpc_session_ptr client_session =
        sim_net->create_client_session(rpc_address::from_host_port("localhost", _test_port));
    client_session->connect();

    rpc_client_session_send(client_session, false);
}

TEST_F(NetProviderTest, AsioNetworkProviderConnectionThreshold)
{
    std::unique_ptr<asio_network_provider_test> asio_network(
        new asio_network_provider_test(task::get_current_rpc(), nullptr));

    error_code start_result;
    start_result = asio_network->start(RPC_CHANNEL_TCP, _test_port, false);
    ASSERT_TRUE(start_result == ERR_OK);

    auto CONN_THRESHOLD = 3;
    LOG_INFO("change FLAGS_conn_threshold_per_ip {} -> {} for test",
             FLAGS_conn_threshold_per_ip,
             CONN_THRESHOLD);
    FLAGS_conn_threshold_per_ip = CONN_THRESHOLD;

    // not exceed threshold
    for (int count = 0; count < CONN_THRESHOLD + 2; count++) {
        LOG_INFO("client # {}", count);
        rpc_session_ptr client_session = asio_network->create_client_session(
            rpc_address::from_host_port("localhost", _test_port));
        client_session->connect();

        rpc_client_session_send(client_session, false);

        client_session->close();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // exceed threshold
    bool reject = false;
    for (int count = 0; count < CONN_THRESHOLD + 2; count++) {
        LOG_INFO("client # {}", count);
        rpc_session_ptr client_session = asio_network->create_client_session(
            rpc_address::from_host_port("localhost", _test_port));
        client_session->connect();

        if (count >= CONN_THRESHOLD)
            reject = true;
        rpc_client_session_send(client_session, reject);
    }
}

} // namespace dsn

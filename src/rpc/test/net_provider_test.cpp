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

#include <boost/asio/detail/impl/reactive_socket_service_base.ipp>
#include <boost/asio/detail/impl/scheduler.ipp>
#include <boost/asio/detail/impl/service_registry.hpp>
// IWYU pragma: no_include <boost/asio/impl/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "rpc/asio_net_provider.h"
#include "rpc/asio_rpc_session.h"
#include "rpc/message_parser.h"
#include "rpc/network.h"
#include "rpc/network.sim.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_engine.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/global_config.h"
#include "runtime/service_engine.h"
#include "runtime/test_utils.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/synchronize.h"

DSN_DECLARE_uint32(conn_threshold_per_ip);
DSN_DECLARE_uint32(conn_pool_max_size);

namespace dsn {

DEFINE_TASK_CODE_RPC(RPC_TEST_NETPROVIDER, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)

void rpc_server_response(dsn::message_ex *request)
{
    std::string str_command;
    ::dsn::unmarshall(request, str_command);
    dsn::message_ex *response = request->create_response();
    ::dsn::marshall(response, str_command);
    dsn_rpc_reply(response);
}

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

    void check_response(bool reject,
                        const std::string &expected_content,
                        dsn::error_code err,
                        dsn::message_ex *req,
                        dsn::message_ex *resp)
    {
        const auto on_completed = defer([this]() { _response_completed.notify(); });

        if (reject) {
            ASSERT_EQ(ERR_TIMEOUT, err);
            return;
        }

        if (err != ERR_OK) {
            LOG_INFO("error msg: {}", err);
            return;
        }

        std::string actual_content;
        ::dsn::unmarshall(resp, actual_content);
        ASSERT_EQ(expected_content, actual_content);
    }

    void test_send(const rpc_session_ptr &client, bool reject)
    {
        message_ex *request = message_ex::create_request(RPC_TEST_NETPROVIDER, 0, 0);

        const std::string expected_content("hello world");
        ::dsn::marshall(request, expected_content);

        rpc_response_task_ptr t(new rpc_response_task(
            request,
            [reject, expected_content, this](
                dsn::error_code err, dsn::message_ex *req, dsn::message_ex *resp) {
                check_response(reject, expected_content, err, req, resp);
            },
            0));
        client->net().engine()->matcher()->on_call(request, t);

        client->send_message(request);
        wait_response();
    }

    void wait_response() { _response_completed.wait(); }

    static int _test_port;

    utils::notify_event _response_completed;
};

int NetProviderTest::_test_port = 20401;

TEST_F(NetProviderTest, AsioNetProvider)
{
    const auto net =
        std::make_unique<tools::asio_network_provider>(task::get_current_rpc(), nullptr);

    ASSERT_EQ(ERR_OK, net->start(RPC_CHANNEL_TCP, _test_port, true));

    // the same asio network handle, start only client is ok
    ASSERT_EQ(ERR_OK, net->start(RPC_CHANNEL_TCP, _test_port, true));

    ASSERT_EQ(_test_port, net->address().port());

    const auto another_net =
        std::make_unique<tools::asio_network_provider>(task::get_current_rpc(), nullptr);
    ASSERT_EQ(ERR_OK, another_net->start(RPC_CHANNEL_TCP, _test_port, true));

    ASSERT_EQ(ERR_OK, another_net->start(RPC_CHANNEL_TCP, _test_port, false));

    ASSERT_EQ(ERR_SERVICE_ALREADY_RUNNING, another_net->start(RPC_CHANNEL_TCP, _test_port, false));

    const auto client =
        net->create_client_session(rpc_address::from_host_port("localhost", _test_port));
    client->connect();

    test_send(client, false);
}

TEST_F(NetProviderTest, AsioUdpProvider)
{
    const auto net = std::make_unique<tools::asio_udp_provider>(task::get_current_rpc(), nullptr);

    ASSERT_EQ(ERR_OK, net->start(RPC_CHANNEL_UDP, 0, true));

    ASSERT_EQ(ERR_OK, net->start(RPC_CHANNEL_UDP, _test_port, false));

    message_ex *request = message_ex::create_request(RPC_TEST_NETPROVIDER, 0, 0);

    const std::string expected_content("hello world");
    ::dsn::marshall(request, expected_content);

    rpc_response_task_ptr t(new rpc_response_task(
        request,
        [expected_content, this](dsn::error_code err, dsn::message_ex *req, dsn::message_ex *resp) {
            check_response(false, expected_content, err, req, resp);
        },
        0));
    net->engine()->matcher()->on_call(request, t);

    net->send_message(request);
    wait_response();
}

TEST_F(NetProviderTest, SimNetProvider)
{
    const auto net =
        std::make_unique<tools::sim_network_provider>(task::get_current_rpc(), nullptr);

    ASSERT_EQ(ERR_OK, net->start(RPC_CHANNEL_TCP, _test_port, false));

    ASSERT_EQ(ERR_ADDRESS_ALREADY_USED, net->start(RPC_CHANNEL_TCP, _test_port, false));

    const auto client =
        net->create_client_session(rpc_address::from_host_port("localhost", _test_port));
    client->connect();

    test_send(client, false);
}

TEST_F(NetProviderTest, AsioNetworkProviderConnectionThreshold)
{
    const auto net =
        std::make_unique<tools::asio_network_provider>(task::get_current_rpc(), nullptr);

    ASSERT_EQ(ERR_OK, net->start(RPC_CHANNEL_TCP, _test_port, false));

    PRESERVE_FLAG(conn_threshold_per_ip);

    constexpr int kConnThreshold{3};
    LOG_INFO("change FLAGS_conn_threshold_per_ip {} -> {} for test",
             FLAGS_conn_threshold_per_ip,
             kConnThreshold);
    FLAGS_conn_threshold_per_ip = kConnThreshold;

    // not exceed threshold
    for (int count = 0; count < kConnThreshold + 2; ++count) {
        LOG_INFO("client # {}", count);
        const auto client =
            net->create_client_session(rpc_address::from_host_port("localhost", _test_port));
        client->connect();

        test_send(client, false);

        client->close();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // exceed threshold
    bool reject = false;
    for (int count = 0; count < kConnThreshold + 2; ++count) {
        LOG_INFO("client # {}", count);
        const auto client =
            net->create_client_session(rpc_address::from_host_port("localhost", _test_port));
        client->connect();

        if (count >= kConnThreshold) {
            reject = true;
        }

        test_send(client, reject);
    }
}

class mock_pool_session : public tools::asio_rpc_session
{
public:
    mock_pool_session(tools::asio_network_provider &net,
                      ::dsn::rpc_address remote_addr,
                      std::shared_ptr<boost::asio::ip::tcp::socket> &socket,
                      message_parser_ptr &parser,
                      bool is_client)
        : asio_rpc_session(net, remote_addr, socket, parser, is_client)
    {
    }

    ~mock_pool_session() override = default;

    void send(uint64_t signature) override
    {
        std::cout << "sending count is " << sending_count() << std::endl;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(mock_pool_session);
    DISALLOW_MOVE_AND_ASSIGN(mock_pool_session);
};

class mock_pool_network : public tools::asio_network_provider
{
public:
    mock_pool_network(rpc_engine *srv, network *inner_provider, int port)
        : tools::asio_network_provider(srv, inner_provider), _port(port)
    {
    }

    rpc_session_ptr create_client_session(::dsn::rpc_address server_addr) override
    {
        auto sock = std::make_shared<boost::asio::ip::tcp::socket>(get_io_service());
        message_parser_ptr parser(new_message_parser(_client_hdr_format));
        return {new mock_pool_session(*this, server_addr, sock, parser, true)};
    }

    void test_conn(uint32_t pool_size)
    {
        PRESERVE_FLAG(conn_pool_max_size);
        FLAGS_conn_pool_max_size = pool_size;

        for (uint32_t i = 0; i < pool_size; ++i) {
            send_request();
            ASSERT_EQ(1, _clients.size());
            ASSERT_EQ(i + 1, _clients.begin()->second->size());
        }

        for (uint32_t i = 0; i < pool_size * 16; ++i) {
            send_request();
            ASSERT_EQ(1, _clients.size());
            ASSERT_EQ(pool_size, _clients.begin()->second->size());
        }
    }

private:
    void send_request()
    {
        message_ex *request = message_ex::create_request(RPC_TEST_NETPROVIDER, 0, 0);
        request->to_address = rpc_address::from_host_port("localhost", _port);

        const std::string expected_content("hello world");
        ::dsn::marshall(request, expected_content);

        send_message(request);
    }

    const int _port;

    DISALLOW_COPY_AND_ASSIGN(mock_pool_network);
    DISALLOW_MOVE_AND_ASSIGN(mock_pool_network);
};

TEST_F(NetProviderTest, GetSessionFromPool)
{
    const auto net =
        std::make_unique<mock_pool_network>(task::get_current_rpc(), nullptr, _test_port);

    ASSERT_EQ(ERR_OK, net->start(RPC_CHANNEL_TCP, _test_port, false));

    net->test_conn(4);
}

} // namespace dsn
